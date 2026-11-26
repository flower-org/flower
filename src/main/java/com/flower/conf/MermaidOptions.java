package com.flower.conf;

/**
 * What goes into a generated Mermaid diagram, besides Steps and transitions. Everything is on by
 * default; notes are wrapped at 24 characters.
 */
public final class MermaidOptions {
  public static final MermaidOptions DEFAULT = builder().build();

  final boolean edgeNotes;
  final boolean edgeNames;
  final boolean combineEdges;
  final boolean stepNotes;
  final boolean flowNote;
  final boolean eventProfiles;
  final boolean stateAccess;
  final boolean wrapNotes;
  final int wrapWidth;

  MermaidOptions(Builder builder) {
    this.edgeNotes = builder.edgeNotes;
    this.edgeNames = builder.edgeNames;
    this.combineEdges = builder.combineEdges;
    this.stepNotes = builder.stepNotes;
    this.flowNote = builder.flowNote;
    this.eventProfiles = builder.eventProfiles;
    this.stateAccess = builder.stateAccess;
    this.wrapNotes = builder.wrapNotes;
    this.wrapWidth = builder.wrapWidth;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** @return labels on transitions, Flow factory and Flow repo links ({@code note} of {@code @StepRef}, {@code @Terminal} etc.) */
  public boolean edgeNotes() {
    return edgeNotes;
  }

  /**
   * @return Transition parameter names on transitions: "name: note" with {@link #edgeNotes()}, or just the name
   *     if there's no note
   */
  public boolean edgeNames() {
    return edgeNames;
  }

  /**
   * @return whether transitions between the same two Steps (or a Step and END) share one edge, with their labels
   *     joined by " / "; otherwise each transition gets an edge of its own
   */
  public boolean combineEdges() {
    return combineEdges;
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

  /**
   * @return whether edge, Step and Event Profile notes are wrapped at {@link #wrapWidth()}, so labels
   *     stay compact. A line break in a note ({@code \n}) always starts a new line.
   */
  public boolean wrapNotes() {
    return wrapNotes;
  }

  /** @return line width, in characters, notes are wrapped at; a longer word gets a line of its own */
  public int wrapWidth() {
    return wrapWidth;
  }

  public static final class Builder {
    boolean edgeNotes = true;
    boolean edgeNames = true;
    boolean combineEdges = true;
    boolean stepNotes = true;
    boolean flowNote = true;
    boolean eventProfiles = true;
    boolean stateAccess = true;
    boolean wrapNotes = true;
    int wrapWidth = 24;

    Builder() {}

    public Builder edgeNotes(boolean edgeNotes) {
      this.edgeNotes = edgeNotes;
      return this;
    }

    public Builder edgeNames(boolean edgeNames) {
      this.edgeNames = edgeNames;
      return this;
    }

    public Builder combineEdges(boolean combineEdges) {
      this.combineEdges = combineEdges;
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

    public Builder wrapNotes(boolean wrapNotes) {
      this.wrapNotes = wrapNotes;
      return this;
    }

    public Builder wrapWidth(int wrapWidth) {
      if (wrapWidth < 1) {
        throw new IllegalArgumentException("wrapWidth must be positive: " + wrapWidth);
      }
      this.wrapWidth = wrapWidth;
      return this;
    }

    public MermaidOptions build() {
      return new MermaidOptions(this);
    }
  }
}
