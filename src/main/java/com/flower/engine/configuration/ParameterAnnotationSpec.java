package com.flower.engine.configuration;

import com.flower.anno.event.EventType;
import com.flower.anno.params.common.EventBus;
import com.flower.anno.params.common.Exec;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.events.EventInfo;
import com.flower.anno.params.events.FlowException;
import com.flower.anno.params.events.FlowInfo;
import com.flower.anno.params.events.InFromFlow;
import com.flower.anno.params.events.StepInfo;
import com.flower.anno.params.events.TransitionInfo;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.step.FlowRepo;
import com.flower.anno.params.transit.InRet;
import com.flower.anno.params.transit.InRetOrException;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowInfoPrm;
import com.flower.conf.FlowRepoPrm;
import com.flower.conf.InOutPrm;
import com.flower.conf.NullableInOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.ReturnValueOrException;
import com.flower.conf.StepInfoPrm;
import com.flower.conf.Transition;
import com.flower.eventbus.FlowEventBus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Describes a Flower parameter annotation: where it can be used, which Java types it accepts, and
 * how it maps to a {@link FunctionParameterRecord}. {@link #ALL} is the single table that
 * {@link FunctionRecord#loadParameter} validates parameters against.
 */
final class ParameterAnnotationSpec<A extends Annotation> {
  /** Order of validation checks; when several fail, the first one is reported. */
  enum CheckOrder {
    FUNCTION_TYPE_FIRST,
    JAVA_TYPE_FIRST
  }

  final Class<A> annotationClass;
  /** Name used in error messages, e.g. "@StepRef". */
  final String displayName;
  final FunctionParameterType parameterType;

  /** Function types the annotation can be used in; null if unrestricted. */
  @Nullable final ImmutableSet<FunctionType> allowedFunctionTypes;
  /** Description of allowed function types for error messages, e.g. "TransitFunction". */
  @Nullable final String allowedFunctionTypesDescription;
  /** Accepted (exact) Java parameter types; null if unrestricted. */
  @Nullable final ImmutableList<Class<?>> allowedJavaTypes;
  /** Event types the annotation can be used with in Event functions; null if unrestricted. */
  @Nullable final ImmutableSet<EventType> allowedEventTypes;

  final CheckOrder checkOrder;
  /** Whether the parameter can be @Nullable. */
  final boolean nullableAllowed;
  /** Whether the parameter must be @Nullable. */
  final boolean nullableRequired;

  final Function<A, String> name;
  /** State field the parameter maps to; null if it doesn't map to a state field. */
  @Nullable final BiFunction<A, Parameter, String> fieldName;
  /** Sets the annotation-specific field of the record. */
  final BiConsumer<FunctionParameterRecord, A> recordSetter;

  private ParameterAnnotationSpec(Builder<A> b) {
    this.annotationClass = b.annotationClass;
    this.displayName = b.displayName;
    this.parameterType = b.parameterType;
    this.allowedFunctionTypes = b.allowedFunctionTypes;
    this.allowedFunctionTypesDescription = b.allowedFunctionTypesDescription;
    this.allowedJavaTypes = b.allowedJavaTypes;
    this.allowedEventTypes = b.allowedEventTypes;
    this.checkOrder = b.checkOrder;
    this.nullableAllowed = b.nullableAllowed;
    this.nullableRequired = b.nullableRequired;
    this.name = b.name;
    this.fieldName = b.fieldName;
    this.recordSetter = b.recordSetter;
  }

  @Nullable
  A get(Parameter prm) {
    return prm.getAnnotation(annotationClass);
  }

  FunctionParameterRecord createRecord(
      A annotation,
      @Nullable Nullable nullableAnnotation,
      Parameter prm,
      Type genericParameterType,
      @Nullable Class<?> flowType) {
    FunctionParameterRecord record =
        new FunctionParameterRecord(
            prm,
            genericParameterType,
            parameterType,
            name.apply(annotation),
            flowType,
            fieldName == null ? null : fieldName.apply(annotation, prm),
            nullableAnnotation);
    recordSetter.accept(record, annotation);
    return record;
  }

  /** Maps to the state field named by {@code field}, or else by the parameter's name. */
  static <A> BiFunction<A, Parameter, String> stateField(
      Function<A, String> field, Function<A, String> name) {
    return (annotation, prm) ->
        StringUtils.defaultIfBlank(
            field.apply(annotation), StringUtils.defaultIfBlank(name.apply(annotation), prm.getName()));
  }

  static <A extends Annotation> Builder<A> spec(
      Class<A> annotationClass,
      FunctionParameterType parameterType,
      Function<A, String> name,
      BiConsumer<FunctionParameterRecord, A> recordSetter) {
    return new Builder<>(annotationClass, parameterType, name, recordSetter);
  }

  static final class Builder<A extends Annotation> {
    final Class<A> annotationClass;
    final FunctionParameterType parameterType;
    final Function<A, String> name;
    final BiConsumer<FunctionParameterRecord, A> recordSetter;
    String displayName;
    @Nullable ImmutableSet<FunctionType> allowedFunctionTypes;
    @Nullable String allowedFunctionTypesDescription;
    @Nullable ImmutableList<Class<?>> allowedJavaTypes;
    @Nullable ImmutableSet<EventType> allowedEventTypes;
    CheckOrder checkOrder = CheckOrder.FUNCTION_TYPE_FIRST;
    boolean nullableAllowed;
    boolean nullableRequired;
    @Nullable BiFunction<A, Parameter, String> fieldName;

    Builder(
        Class<A> annotationClass,
        FunctionParameterType parameterType,
        Function<A, String> name,
        BiConsumer<FunctionParameterRecord, A> recordSetter) {
      this.annotationClass = annotationClass;
      this.parameterType = parameterType;
      this.name = name;
      this.recordSetter = recordSetter;
      this.displayName = "@" + annotationClass.getSimpleName();
    }

    Builder<A> displayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    Builder<A> onlyIn(String description, FunctionType... functionTypes) {
      this.allowedFunctionTypes = ImmutableSet.copyOf(functionTypes);
      this.allowedFunctionTypesDescription = description;
      return this;
    }

    Builder<A> javaType(Class<?>... javaTypes) {
      this.allowedJavaTypes = ImmutableList.copyOf(javaTypes);
      return this;
    }

    Builder<A> javaTypeCheckedFirst() {
      this.checkOrder = CheckOrder.JAVA_TYPE_FIRST;
      return this;
    }

    Builder<A> onlyForEventTypes(EventType... eventTypes) {
      this.allowedEventTypes = ImmutableSet.copyOf(eventTypes);
      return this;
    }

    Builder<A> nullableAllowed() {
      this.nullableAllowed = true;
      return this;
    }

    Builder<A> nullableRequired() {
      this.nullableAllowed = true;
      this.nullableRequired = true;
      return this;
    }

    Builder<A> fieldName(BiFunction<A, Parameter, String> fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    ParameterAnnotationSpec<A> build() {
      return new ParameterAnnotationSpec<>(this);
    }
  }

  /**
   * All Flower parameter annotations. The order matters for error messages: it's the order in which
   * conflicting annotations on the same parameter are reported.
   */
  static final ImmutableList<ParameterAnnotationSpec<?>> ALL =
      ImmutableList.of(
          spec(In.class, FunctionParameterType.IN, In::name, (r, a) -> r.inAnnotation = a)
              .nullableAllowed()
              .fieldName(stateField(In::from, In::name))
              .build(),
          spec(Out.class, FunctionParameterType.OUT, Out::name, (r, a) -> r.outAnnotation = a)
              .javaType(OutPrm.class)
              .fieldName(stateField(Out::to, Out::name))
              .build(),
          spec(
                  InOut.class,
                  FunctionParameterType.IN_OUT,
                  InOut::name,
                  (r, a) -> r.inOutAnnotation = a)
              .javaType(InOutPrm.class, NullableInOutPrm.class)
              .fieldName(stateField(InOut::fromAndTo, InOut::name))
              .build(),
          spec(Exec.class, FunctionParameterType.EXEC, Exec::name, (r, a) -> r.execAnnotation = a)
              .javaType(Executor.class)
              .build(),
          spec(
                  EventBus.class,
                  FunctionParameterType.EVENT_BUS,
                  EventBus::name,
                  (r, a) -> r.eventBusAnnotation = a)
              .onlyIn(
                  "StepFunction, TransitFunction or StepAndTransitFunction",
                  FunctionType.STEP,
                  FunctionType.TRANSIT,
                  FunctionType.STEP_AND_TRANSIT,
                  FunctionType.GLOBAL)
              .javaType(FlowEventBus.class)
              .build(),
          spec(
                  StepRef.class,
                  FunctionParameterType.STEP_REF,
                  StepRef::name,
                  (r, a) -> r.stepRefAnnotation = a)
              .onlyIn(
                  "TransitFunction",
                  FunctionType.TRANSIT,
                  FunctionType.STEP_AND_TRANSIT,
                  FunctionType.GLOBAL)
              .javaType(Transition.class)
              .build(),
          spec(
                  Terminal.class,
                  FunctionParameterType.TERMINAL,
                  Terminal::name,
                  (r, a) -> r.terminalAnnotation = a)
              .onlyIn(
                  "TransitFunction",
                  FunctionType.TRANSIT,
                  FunctionType.STEP_AND_TRANSIT,
                  FunctionType.GLOBAL)
              .javaType(Transition.class)
              .build(),
          spec(
                  InRet.class,
                  FunctionParameterType.IN_RET,
                  InRet::name,
                  (r, a) -> r.inRetAnnotation = a)
              .onlyIn("TransitFunction", FunctionType.TRANSIT, FunctionType.GLOBAL)
              .nullableAllowed()
              .build(),
          spec(
                  InRetOrException.class,
                  FunctionParameterType.IN_RET_OR_EXCEPTION,
                  InRetOrException::name,
                  (r, a) -> r.inRetOrExceptionAnnotation = a)
              .onlyIn("TransitFunction", FunctionType.TRANSIT, FunctionType.GLOBAL)
              .javaType(ReturnValueOrException.class)
              .build(),
          spec(
                  FlowFactory.class,
                  FunctionParameterType.FLOW_TYPE_FACTORY,
                  FlowFactory::name,
                  (r, a) -> r.flowFactoryAnnotation = a)
              .displayName("@FlowTypeFactory")
              .onlyIn(
                  "StepFunction",
                  FunctionType.STEP,
                  FunctionType.STEP_AND_TRANSIT,
                  FunctionType.GLOBAL)
              .javaType(FlowFactoryPrm.class)
              .build(),
          spec(
                  FlowRepo.class,
                  FunctionParameterType.FLOW_REPO,
                  FlowRepo::name,
                  (r, a) -> r.flowRepoAnnotation = a)
              .onlyIn(
                  "StepFunction",
                  FunctionType.STEP,
                  FunctionType.STEP_AND_TRANSIT,
                  FunctionType.GLOBAL)
              .javaType(FlowRepoPrm.class)
              .build(),
          spec(
                  InFromFlow.class,
                  FunctionParameterType.IN_FROM_FLOW,
                  InFromFlow::name,
                  (r, a) -> r.inFromFlowAnnotation = a)
              .nullableRequired()
              .fieldName(stateField(InFromFlow::from, InFromFlow::name))
              .build(),
          spec(
                  EventInfo.class,
                  FunctionParameterType.EVENT_INFO,
                  EventInfo::name,
                  (r, a) -> r.eventInfoAnnotation = a)
              .javaType(EventType.class)
              .onlyIn("EventFunction", FunctionType.EVENT, FunctionType.GLOBAL)
              .javaTypeCheckedFirst()
              .build(),
          spec(
                  FlowInfo.class,
                  FunctionParameterType.FLOW_INFO,
                  FlowInfo::name,
                  (r, a) -> r.flowInfoAnnotation = a)
              .javaType(FlowInfoPrm.class)
              .onlyIn("EventFunction", FunctionType.EVENT, FunctionType.GLOBAL)
              .javaTypeCheckedFirst()
              .build(),
          spec(
                  StepInfo.class,
                  FunctionParameterType.STEP_INFO,
                  StepInfo::name,
                  (r, a) -> r.stepInfoAnnotation = a)
              .javaType(StepInfoPrm.class)
              .onlyIn("EventFunction", FunctionType.EVENT, FunctionType.GLOBAL)
              .javaTypeCheckedFirst()
              .onlyForEventTypes(
                  EventType.BEFORE_STEP,
                  EventType.AFTER_STEP,
                  EventType.BEFORE_STEP_ITERATION,
                  EventType.AFTER_STEP_ITERATION,
                  EventType.BEFORE_EXEC,
                  EventType.AFTER_EXEC,
                  EventType.BEFORE_TRANSIT,
                  EventType.AFTER_TRANSIT)
              .build(),
          spec(
                  TransitionInfo.class,
                  FunctionParameterType.TRANSITION_INFO,
                  TransitionInfo::name,
                  (r, a) -> r.transitionInfoAnnotation = a)
              .javaType(Transition.class)
              .onlyIn("EventFunction", FunctionType.EVENT, FunctionType.GLOBAL)
              .javaTypeCheckedFirst()
              .onlyForEventTypes(EventType.AFTER_TRANSIT /*, AFTER_STEP_ITERATION, AFTER_STEP*/)
              .build(),
          spec(
                  FlowException.class,
                  FunctionParameterType.FLOW_EXCEPTION,
                  FlowException::name,
                  (r, a) -> r.flowExceptionAnnotation = a)
              .javaType(Throwable.class)
              .onlyIn("EventFunction", FunctionType.EVENT, FunctionType.GLOBAL)
              .javaTypeCheckedFirst()
              .onlyForEventTypes(EventType.FLOW_EXCEPTION)
              .build());
}
