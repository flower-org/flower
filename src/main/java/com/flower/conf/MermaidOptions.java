package com.flower.conf;

/**
 * What goes into a generated Mermaid diagram, besides Steps and transitions. Everything is on by
 * default except {@link Builder#stateAccess(boolean) state access}, which is useful but noisy.
 */
public final class MermaidOptions {
  public static final MermaidOptions DEFAULT = builder().build();

  final boolean edgeNotes;
  final boolean stepNotes;
  final boolean flowNote;
  final boolean eventProfiles;
  final boolean stateAccess;

  MermaidOptions(Builder builder) {
    this.edgeNotes = builder.edgeNotes;
    this.stepNotes = builder.stepNotes;
    this.flowNote = builder.flowNote;
    this.eventProfiles = builder.eventProfiles;
    this.stateAccess = builder.stateAccess;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** @return labels on transitions, Flow factory and Flow repo links ({@code note} of {@code @StepRef}, {@code @Terminal} etc.) */
  public boolean edgeNotes() {
    return edgeNotes;
  }

  /** @return Step notes under Step names */
  public boolean stepNotes() {
    return stepNotes;
  }

  /** @return the Flow note as the diagram title */
  public boolean flowNote() {
    return flowNote;
  }

  /** @return Event Profile side nodes, with their notes */
  public boolean eventProfiles() {
    return eventProfiles;
  }

  /** @return State fields each Step reads and writes (@In, @Out, @InOut, returnTo), under Step names */
  public boolean stateAccess() {
    return stateAccess;
  }

  public static final class Builder {
    boolean edgeNotes = true;
    boolean stepNotes = true;
    boolean flowNote = true;
    boolean eventProfiles = true;
    boolean stateAccess = false;

    Builder() {}

    public Builder edgeNotes(boolean edgeNotes) {
      this.edgeNotes = edgeNotes;
      return this;
    }

    public Builder stepNotes(boolean stepNotes) {
      this.stepNotes = stepNotes;
      return this;
    }

    public Builder flowNote(boolean flowNote) {
      this.flowNote = flowNote;
      return this;
    }

    public Builder eventProfiles(boolean eventProfiles) {
      this.eventProfiles = eventProfiles;
      return this;
    }

    public Builder stateAccess(boolean stateAccess) {
      this.stateAccess = stateAccess;
      return this;
    }

    public MermaidOptions build() {
      return new MermaidOptions(this);
    }
  }
}
