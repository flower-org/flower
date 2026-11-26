package com.flower.engine.runner;

import static com.flower.utilities.FuturesTool.directExecutor;

import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.FlowType;
import com.flower.conf.FlowExec;
import com.flower.conf.FlowExecCallback;
import com.flower.conf.FlowFuture;
import com.flower.conf.FlowId;
import com.flower.conf.InternalFlowExec;
import com.flower.conf.MermaidOptions;
import com.flower.conf.StateSerializer;
import com.flower.conf.StepInfoPrm;
import com.flower.engine.FlowImpl;
import com.flower.engine.FlowerId;
import com.flower.engine.runner.event.EventProfileForFlowTypeCallContext;
import com.flower.engine.runner.step.InternalTransition;
import com.flower.engine.runner.step.StepCallContext;
import com.flower.utilities.FlowerException;
import com.flower.utilities.FutureCombiner;
import com.flower.utilities.FuturesTool;
import com.google.common.base.Preconditions;

import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;

public class FlowExecImpl<T> implements InternalFlowExec<T> {
  static final String BEGIN = "BEGIN([BEGIN])";
  static final String END = "END([END])";
  static final String DIAGRAM_NAME = "flowchart";

  static final String LINK_END = "-->";
  static final String FLOW_FACTORY_LINK_END = ".->";
  static final String FLOW_REPO_LINK_END = ".->";

  final FlowRunner flowRunner;
  final Class<T> flowType;
  final FlowCallContext flowCallContext;
  final FlowExecCallback flowExecCallback;
  final ScheduledExecutorService scheduler;
  @Nullable final StateSerializer<T> stateSerializer;

  public FlowExecImpl(
      FlowRunner flowRunner,
      Class<T> flowType,
      FlowCallContext flowCallContext,
      FlowExecCallback flowExecCallback,
      ScheduledExecutorService scheduler,
      @Nullable StateSerializer<T> stateSerializer) {
    this.flowRunner = flowRunner;
    this.flowType = flowType;
    this.flowCallContext = flowCallContext;
    this.flowExecCallback = flowExecCallback;
    this.scheduler = scheduler;
    this.stateSerializer = stateSerializer;
  }

  @Override
  public FlowFuture<T> runChildFlow(FlowId parentFlowId, T flow, Duration startupDelay) {
    // TODO: decouple FlowerId - FlowId factory?
    final FlowerId flowId = new FlowerId((FlowerId) parentFlowId);
    return runFlow(flowId, flow, startupDelay);
  }

  @Override
  public FlowFuture<T> runFlow(T flow, Duration startupDelay) {
    final FlowerId flowId = new FlowerId();
    return runFlow(flowId, flow, startupDelay);
  }

  public FlowFuture<T> runFlow(FlowId flowId, T flow, Duration startupDelay) {
    // TODO: implement
    throw new UnsupportedOperationException("Running flow with startup delay is not implemented yet");
  }

  @Override
  public FlowFuture<T> runChildFlow(FlowId parentFlowId, T flow) {
    // TODO: decouple FlowerId - FlowId factory?
    final FlowerId flowId = new FlowerId((FlowerId) parentFlowId);
    return runFlow(flowId, flow);
  }

  @Override
  public FlowFuture<T> runFlow(T flowState) {
    final FlowerId flowId = new FlowerId();
    return runFlow(flowId, flowState);
  }

  public FlowFuture<T> runFlow(FlowId flowId, T flowState) {
    if (!flowState.getClass().equals(flowType)) {
      //Inherited flows will have a different executor.
      FlowExec childExec = flowRunner.getFlowExec(flowState.getClass());
      return childExec.runFlow(flowState);
    }

    FlowImpl<T> flow_ = null;
    CompletableFuture<T> checkReturnValue;
    try {
      final FlowImpl<T> flow = flow_ = flowCallContext.createFlow(flowId, flowState);

      final Pair<FlowImpl<T>, CompletableFuture<InternalTransition>> flowContextPair =
          flowCallContext.runFirstStep(flow);

      final FlowImpl<T> flowInstance = flowContextPair.getLeft();
      final CompletableFuture<InternalTransition> firstStepCallFuture = flowContextPair.getRight();

      CompletableFuture<T> settableFlowFuture = new CompletableFuture<>();
      //Recursive flow execution routine, executes the flow
      transitionToNextStep(
          flowInstance,
          firstStepCallFuture,
          flowCallContext.flowName,
          flowCallContext.firstStepName,
          settableFlowFuture);

      // AFTER_FLOW / FLOW_EXCEPTION events are awaited like any other event: the Flow completes once
      // their blocking stages (FINALIZER, SYNCHRONIZED, BLOCKING) are done; NON_BLOCKING ones aren't
      // awaited.
      CompletableFuture<T> flowFuture =
          FuturesTool.transformAsync(
              settableFlowFuture,
              t -> {
                flow.setStepInfo(null);
                // A failing FINALIZER fails the Flow (which then runs FLOW_EXCEPTION events)
                return FuturesTool.transform(
                    flowCallContext.runEvents(EventType.AFTER_FLOW, flow, flow, null, null),
                    void_ -> t,
                    directExecutor());
              },
              scheduler);

      checkReturnValue =
          FuturesTool.catchingAsync(
              flowFuture, Throwable.class, t -> runFlowExceptionEvents(flow, t), scheduler);

      flowExecCallback.flowStarted(flowId, checkReturnValue);

      CompletableFuture<Void> unusedFinished =
          FuturesTool.whenAllCompleteRun(
              List.of(checkReturnValue), () -> flowExecCallback.flowFinished(flowId), scheduler);

      return new SimpleFlowFuture<>(flowId, checkReturnValue);
    } catch (Throwable t) {
      if (flow_ != null) {
        return new SimpleFlowFuture<>(flowId, runFlowExceptionEvents(flow_, t));
      }
      return new SimpleFlowFuture<>(flowId, CompletableFuture.failedFuture(t));
    }
  }

  /**
   * Runs FLOW_EXCEPTION events and returns a future that fails with {@code flowException} once their
   * blocking stages are done. If a FINALIZER fails, its exception is added to {@code flowException}
   * as suppressed, so it isn't lost.
   */
  CompletableFuture<T> runFlowExceptionEvents(FlowImpl<T> flow, Throwable flowException) {
    flow.setStepInfo(null);
    CompletableFuture<Void> events;
    try {
      events = flowCallContext.runEvents(EventType.FLOW_EXCEPTION, flow, flow, null, flowException);
    } catch (Throwable t) {
      events = CompletableFuture.failedFuture(t);
    }
    CompletableFuture<T> result = new CompletableFuture<>();
    CompletableFuture<Void> unused =
        events.whenComplete(
            (void_, eventsException) -> {
              if (eventsException != null) {
                Throwable cause = FuturesTool.unwrap(eventsException);
                if (cause != flowException) {
                  flowException.addSuppressed(cause);
                }
              }
              result.completeExceptionally(flowException);
            });
    return result;
  }

  @Override
  public String buildMermaidGraph(boolean addHeader, MermaidOptions options) {
    Set<String> existingLinks = new HashSet<>();

    StringBuilder graphBuilder = new StringBuilder();
    if (addHeader) {
      graphBuilder.append("- ").append(flowType.getSimpleName()).append("\n");
    }
    graphBuilder
        .append("```mermaid\n")
        .append(options.flowNote() ? createTitle(flowType) : "")
        .append(DIAGRAM_NAME + "\n")
        .append(BEGIN)
        .append('\n')
        .append(END)
        .append('\n');

    for (Map.Entry<String, StepCallContext> entry : flowCallContext.stepCalls.entrySet()) {
      StepCallContext step = entry.getValue();
      String stepName = step.getStepInfo().stepName();
      boolean isFirst = step.getStepInfo().isFirstStep();
      List<InternalTransition> transitions = step.getTransitions();

      List<String> nodeLines = new ArrayList<>();
      if (options.stepNotes() && StringUtils.isNotBlank(step.getNote())) {
        nodeLines.add(italic(wrap(options, step.getNote())));
      }
      if (options.stateAccess()) {
        nodeLines.addAll(stateAccessLines(step.getStateReads(), step.getStateWrites()));
      }
      graphBuilder.append(createNode(stepName, nodeLines)).append('\n');
      if (isFirst) {
        String link = createLink(BEGIN, stepName, null);
        if (existingLinks.add(link)) { graphBuilder.append(link).append('\n'); }
      }

      if (options.combineEdges()) {
        // Target -> labels of all transitions to it, in declaration order
        Map<String, List<List<List<Word>>>> combined = new LinkedHashMap<>();
        for (InternalTransition transition : transitions) {
          List<List<List<Word>>> labels = combined.computeIfAbsent(transitionTarget(transition), t -> new ArrayList<>());
          List<List<Word>> label = transitionWords(options, transition);
          if (!label.isEmpty() && !labels.contains(label)) { labels.add(label); }
        }
        for (Map.Entry<String, List<List<List<Word>>>> target : combined.entrySet()) {
          String link = createLink(stepName, target.getKey(), renderWords(options, joinLabels(target.getValue())));
          if (existingLinks.add(link)) { graphBuilder.append(link).append('\n'); }
        }
      } else {
        for (InternalTransition transition : transitions) {
          String link = createLink(stepName, transitionTarget(transition), transitionLabel(options, transition));
          if (existingLinks.add(link)) { graphBuilder.append(link).append('\n'); }
        }
      }

      for (Pair<String, String> flowFactory : step.getFlowFactories()) {
          String link = createFlowFactoryLink(stepName, "Factory:" + getSimpleClassName(flowFactory.getKey()), edgeNote(options, flowFactory.getValue()));
          graphBuilder.append(link).append('\n');
      }
      for (Pair<String, String> flowRepo : step.getFlowRepos()) {
          String link = createFlowRepoLink(stepName, "Repo:" + getSimpleClassName(flowRepo.getKey()), edgeNote(options, flowRepo.getValue()));
          graphBuilder.append(link).append('\n');
      }
    }

    if (options.eventProfiles()) {
      flowCallContext.eventContexts.values().stream()
          .sorted(Comparator.comparing(EventProfileForFlowTypeCallContext::getEventProfileName))
          .forEach(profile -> graphBuilder.append(createEventProfileNode(profile, options)).append('\n'));
    }

    graphBuilder.append("```");
    if (addHeader) {
      graphBuilder.append("\n");
    }

    return graphBuilder.toString();
  }

  static String getSimpleClassName(String className) {
    if (StringUtils.isBlank(className)) {
      return "";
    }
    int lastDotIndex = className.lastIndexOf('.');
    if (lastDotIndex == -1) {
      return className;
    }
    return className.substring(lastDotIndex + 1);
  }

  /** Quoted Mermaid edge label, so spaces and punctuation in notes don't break parsing. */
  static String label(String note) {
    return "\"" + note.replace("\"", "#quot;") + "\"";
  }

  /** Mermaid front matter with the Flow's note as title, empty if none. */
  static String createTitle(Class<?> flowType) {
    FlowType flowTypeAnnotation = flowType.getAnnotation(FlowType.class);
    if (flowTypeAnnotation == null || StringUtils.isBlank(flowTypeAnnotation.note())) {
      return "";
    }
    String title = flowTypeAnnotation.note().replace("\\", "\\\\").replace("\"", "\\\"");
    return "---\ntitle: \"" + title + "\"\n---\n";
  }

  static String transitionTarget(InternalTransition transition) {
    return transition.isTerminal() ? END : Preconditions.checkNotNull(transition.getStepName());
  }

  static @Nullable String edgeNote(MermaidOptions options, @Nullable String note) {
    return options.edgeNotes() && StringUtils.isNotBlank(note) ? italic(wrap(options, note)) : null;
  }

  /** Transition label: "<b>name:</b> <i>note</i>", or whichever of the two is present and enabled. */
  static @Nullable String transitionLabel(MermaidOptions options, InternalTransition transition) {
    return renderWords(options, transitionWords(options, transition));
  }

  /** Transition label as paragraphs of styled words, empty if there's nothing to show. */
  static List<List<Word>> transitionWords(MermaidOptions options, InternalTransition transition) {
    String name = options.edgeNames() ? StringUtils.trimToNull(transition.getParameterName()) : null;
    String note = options.edgeNotes() ? StringUtils.trimToNull(transition.getNote()) : null;
    List<List<Word>> paragraphs = new ArrayList<>();
    if (note != null) {
      for (String paragraph : note.split("\n", -1)) {
        paragraphs.add(words(paragraph, "i"));
      }
    }
    if (name != null) {
      if (paragraphs.isEmpty()) {
        paragraphs.add(new ArrayList<>());
      }
      paragraphs.get(0).add(0, new Word(note == null ? name : name + ":", "b"));
    }
    return paragraphs;
  }

  /** Labels of one combined edge as a single text, joined by " / ", so they wrap together. */
  static List<List<Word>> joinLabels(List<List<List<Word>>> labels) {
    List<List<Word>> joined = new ArrayList<>();
    for (List<List<Word>> label : labels) {
      if (joined.isEmpty()) {
        label.forEach(paragraph -> joined.add(new ArrayList<>(paragraph)));
        continue;
      }
      List<Word> last = joined.get(joined.size() - 1);
      last.add(new Word("/", null));
      last.addAll(label.get(0));
      label.subList(1, label.size()).forEach(paragraph -> joined.add(new ArrayList<>(paragraph)));
    }
    return joined;
  }

  static List<Word> words(String text, @Nullable String tag) {
    List<Word> words = new ArrayList<>();
    for (String word : text.trim().split("\\s+")) {
      if (!word.isEmpty()) {
        words.add(new Word(word, tag));
      }
    }
    return words;
  }

  /**
   * Paragraphs of styled words, wrapped like {@link #wrap}; each line opens and closes its own tags.
   * @return null if there are no paragraphs
   */
  static @Nullable String renderWords(MermaidOptions options, List<List<Word>> paragraphs) {
    if (paragraphs.isEmpty()) {
      return null;
    }
    List<String> lines = new ArrayList<>();
    for (List<Word> paragraph : paragraphs) {
      int[] lengths = paragraph.stream().mapToInt(word -> word.text.length()).toArray();
      int from = 0;
      for (int count : wordsPerLine(options, lengths)) {
        lines.add(renderLine(paragraph.subList(from, from + count)));
        from += count;
      }
    }
    return String.join("<br/>", lines);
  }

  /** Words joined by spaces, a tag around each run of words in the same style. */
  static String renderLine(List<Word> words) {
    StringBuilder line = new StringBuilder();
    String open = null;
    for (Word word : words) {
      if (!Objects.equals(word.tag, open)) {
        if (open != null) { line.append("</").append(open).append('>'); }
        if (line.length() > 0) { line.append(' '); }
        if (word.tag != null) { line.append('<').append(word.tag).append('>'); }
        open = word.tag;
      } else if (line.length() > 0) {
        line.append(' ');
      }
      line.append(word.text);
    }
    if (open != null) { line.append("</").append(open).append('>'); }
    return line.toString();
  }

  /** A label word, with the HTML tag it's wrapped in ({@code b}, {@code i}), or null for plain. */
  static final class Word {
    final String text;
    final @Nullable String tag;

    Word(String text, @Nullable String tag) {
      this.text = text;
      this.tag = tag;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Word word && text.equals(word.text) && Objects.equals(tag, word.tag);
    }

    @Override
    public int hashCode() {
      return Objects.hash(text, tag);
    }
  }

  /** Notes are comments: italic, to tell them apart from names (bold) and State access (plain). */
  static String italic(String text) {
    return "<i>" + text + "</i>";
  }

  /**
   * Note with {@code <br/>} line breaks: at each {@code \n}, and, if wrapping is on, at word boundaries.
   * Lines are balanced: as few as {@code wrapWidth} allows, as even in length as possible.
   */
  static String wrap(MermaidOptions options, String note) {
    List<String> lines = new ArrayList<>();
    for (String paragraph : note.split("\n", -1)) {
      if (!options.wrapNotes()) {
        lines.add(paragraph);
        continue;
      }
      String[] words = paragraph.trim().split("\\s+");
      int from = 0;
      for (int count : wordsPerLine(options, Arrays.stream(words).mapToInt(String::length).toArray())) {
        lines.add(String.join(" ", Arrays.asList(words).subList(from, from + count)));
        from += count;
      }
    }
    return String.join("<br/>", lines);
  }

  /** Words per line, balanced: as few lines as {@code wrapWidth} allows, as even in length as possible. */
  static List<Integer> wordsPerLine(MermaidOptions options, int[] lengths) {
    if (!options.wrapNotes()) {
      return List.of(lengths.length);
    }
    List<Integer> wrapped = wrapWords(lengths, options.wrapWidth());
    for (int width = 1; width < options.wrapWidth(); width++) {
      List<Integer> narrower = wrapWords(lengths, width);
      if (narrower.size() <= wrapped.size()) {
        return narrower;
      }
    }
    return wrapped;
  }

  /** Greedy word wrap, words per line; a word longer than {@code width} gets a line of its own. */
  static List<Integer> wrapWords(int[] lengths, int width) {
    List<Integer> counts = new ArrayList<>();
    int line = 0;
    int count = 0;
    for (int length : lengths) {
      if (count > 0 && line + 1 + length > width) {
        counts.add(count);
        line = 0;
        count = 0;
      }
      line += (count > 0 ? 1 : 0) + length;
      count++;
    }
    counts.add(count);
    return counts;
  }

  /** "in: ...", "out: ...", "inout: ..." lines for the State fields a Step reads and writes; empty groups are left out. */
  static List<String> stateAccessLines(Set<String> reads, Set<String> writes) {
    List<String> in = reads.stream().filter(f -> !writes.contains(f)).toList();
    List<String> out = writes.stream().filter(f -> !reads.contains(f)).toList();
    List<String> inOut = reads.stream().filter(writes::contains).toList();
    List<String> lines = new ArrayList<>();
    if (!in.isEmpty()) { lines.add("in: " + String.join(", ", in)); }
    if (!out.isEmpty()) { lines.add("out: " + String.join(", ", out)); }
    if (!inOut.isEmpty()) { lines.add("inout: " + String.join(", ", inOut)); }
    return lines;
  }

  /** Step node, name in bold; extra lines (note, State access) go under it. Edges refer to steps by bare id. */
  static String createNode(String stepName, List<String> lines) {
    StringBuilder text = new StringBuilder("<b>").append(stepName).append("</b>");
    for (String line : lines) {
      text.append("<br/>").append(line.replace("\"", "#quot;"));
    }
    return String.format("%s[[\"%s\"]]", stepName, text);
  }

  /** Event Profile as a side node (not linked: its Events fire around every Step), with its note, if any. */
  static String createEventProfileNode(EventProfileForFlowTypeCallContext profile, MermaidOptions options) {
    Class<?> containerType = profile.getEventProfileContainerType();
    String name = getSimpleClassName(profile.getEventProfileName());
    EventProfileContainer container = containerType.getAnnotation(EventProfileContainer.class);
    String note = container == null ? "" : container.note();
    String text = "<b>" + name + "</b>"
        + (StringUtils.isBlank(note) ? "" : "<br/>" + italic(wrap(options, note)).replace("\"", "#quot;"));
    return String.format("EventProfile_%s[/\"%s\"/]", containerType.getSimpleName(), text);
  }

  String createLink(String left, String right, @Nullable String note) {
    if (StringUtils.isBlank(note)) {
      return String.format("%s %s %s", left, LINK_END, right);
    }
    return String.format("%s %s|%s| %s", left, LINK_END, label(note), right);
  }

  String createFlowFactoryLink(String left, String right, @Nullable String note) {
    if (StringUtils.isBlank(note)) {
      return String.format("%s %s %s>%s]", left, FLOW_FACTORY_LINK_END, right, right);
    }
    return String.format("%s %s|%s| %s>%s]", left, FLOW_FACTORY_LINK_END, label(note), right, right);
  }

  String createFlowRepoLink(String left, String right, @Nullable String note) {
    if (StringUtils.isBlank(note)) {
      return String.format("%s %s %s{{%s}}", left, FLOW_REPO_LINK_END, right, right);
    }
    return String.format("%s %s|%s| %s{{%s}}", left, FLOW_REPO_LINK_END, label(note), right, right);
  }

  CompletableFuture<T> runEventsForLastStep(FlowImpl<T> flowInstance) {
    return FuturesTool.transform(
        flowCallContext.runEvents(EventType.AFTER_STEP, flowInstance, flowInstance, null, null),
        void_ -> flowInstance.getState(),
        scheduler);
  }

  void transitionToNextStep(
      FlowImpl<T> flowInstance,
      CompletableFuture<InternalTransition> stepCallFuture,
      String flowTypeName,
      String stepName,
      CompletableFuture<T> settableFuture) {
    CompletableFuture<Void> unused =
        FuturesTool.whenAllCompleteRun(
            List.of(stepCallFuture),
            () -> {
              try {
                InternalTransition transition = stepCallFuture.get();
                if (transition == null) {
                  throw new FlowerException(
                      String.format("Step [%s] of Flow type [%s] provided `null` Transition object.",
                              stepName, flowTypeName));
                }

                // 1. Check final state in the form of transition to Terminal step
                if (transition.isTerminal()) {
                  if (transition.getDelay() != null) {
                    Duration delay = transition.getDelay();

                    // Flow finished after a delay
                    FuturesTool.assignSettableFuture(
                        FutureCombiner.delayExecutionAsync(
                            scheduler,
                            delay.toNanos(),
                            TimeUnit.NANOSECONDS,
                            () -> runEventsForLastStep(flowInstance)),
                        settableFuture,
                        scheduler);
                  } else {
                    // Flow finished
                    FuturesTool.assignSettableFuture(
                        runEventsForLastStep(flowInstance), settableFuture, scheduler);
                  }
                } else {
                  transitionToNextStep0(flowInstance, transition, settableFuture);
                }
              } catch (ExecutionException e) {
                settableFuture.completeExceptionally(e.getCause() != null ? e.getCause() : e);
              } catch (Throwable t) {
                settableFuture.completeExceptionally(t);
              }
            },
            scheduler);
  }

  void transitionToNextStep0(
      FlowImpl<T> flowInstance, InternalTransition transition, CompletableFuture<T> settableFuture) {
    if (transition.getDelay() != null) {
      Duration delay = transition.getDelay();
      CompletableFuture<Void> delayedStep =
          FutureCombiner.delayExecution(
              scheduler,
              delay.toNanos(),
              TimeUnit.NANOSECONDS,
              () -> {
                runNextStep(
                    flowInstance,
                    Preconditions.checkNotNull(transition.getStepName()),
                    settableFuture);
                return null;
              });
      // If the delayed step couldn't be started (scheduler rejection, exception in runNextStep),
      // fail the flow instead of leaving it hanging.
      CompletableFuture<?> unused =
          delayedStep.whenComplete(
              (v, t) -> {
                if (t != null) {
                  settableFuture.completeExceptionally(FuturesTool.unwrap(t));
                }
              });
    } else {
      runNextStep(
          flowInstance, Preconditions.checkNotNull(transition.getStepName()), settableFuture);
    }
  }

  void runNextStep(
      FlowImpl<T> flowInstance, String nextStepName, CompletableFuture<T> settableFuture) {
    String currentStepName = flowInstance.getCurrentStep();

    CompletableFuture<InternalTransition> stepCallFuture;
    // Step changed
    if (!currentStepName.equals(nextStepName)) {
      CompletableFuture<Void> beforeStep =
          FuturesTool.transformAsync(
              flowCallContext.runEvents(
                  EventType.AFTER_STEP, flowInstance, flowInstance, null, null),
              void_ -> {
                // TODO: some duplication here?
                flowInstance.setCurrentStep(nextStepName);
                StepInfoPrm stepInfo = flowCallContext.getStep(nextStepName).getStepInfo();
                flowInstance.setStepInfo(stepInfo);
                return flowCallContext.runEvents(
                    EventType.BEFORE_STEP, flowInstance, flowInstance, null, null);
              },
              scheduler);
      stepCallFuture =
          FuturesTool.transformAsync(
              beforeStep, void_ -> flowCallContext.runStep(flowInstance, nextStepName), scheduler);
    } else {
      stepCallFuture = flowCallContext.runStep(flowInstance, nextStepName);
    }

    transitionToNextStep(
        flowInstance, stepCallFuture, flowCallContext.flowName, nextStepName, settableFuture);
  }

  public Class<T> getFlowType() {
    return flowType;
  }

  @Nullable
  @Override
  public StateSerializer<T> getStateSerializer() {
    return stateSerializer;
  }
}
