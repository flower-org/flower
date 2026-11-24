package com.flower.engine.configuration;

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
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

public class FunctionParameterRecord {
  @Nullable public In inAnnotation = null;
  @Nullable public Out outAnnotation = null;
  @Nullable public InOut inOutAnnotation = null;
  @Nullable public Exec execAnnotation = null;
  @Nullable public EventBus eventBusAnnotation = null;

  @Nullable public InFromFlow inFromFlowAnnotation = null;

  @Nullable public StepRef stepRefAnnotation = null;
  @Nullable public Terminal terminalAnnotation = null;
  @Nullable public InRet inRetAnnotation = null;
  @Nullable public InRetOrException inRetOrExceptionAnnotation = null;
  @Nullable public FlowFactory flowFactoryAnnotation = null;
  @Nullable public FlowRepo flowRepoAnnotation = null;

  @Nullable public EventInfo eventInfoAnnotation = null;
  @Nullable public FlowInfo flowInfoAnnotation = null;
  @Nullable public StepInfo stepInfoAnnotation = null;
  @Nullable public TransitionInfo transitionInfoAnnotation = null;
  @Nullable public FlowException flowExceptionAnnotation = null;

  @Nullable public final Nullable nullableAnnotation;

  @Nullable public final Class<?> flowType;
  public final FunctionParameterType type;
  public final String name;
  @Nullable public final String fieldName;
  public final Parameter prm;
  public final Type genericParameterType;

  FunctionParameterRecord(
      Parameter prm,
      Type genericParameterType,
      FunctionParameterType type,
      String name,
      @Nullable Class<?> flowType,
      @Nullable String fieldName,
      @Nullable Nullable nullableAnnotation) {
    this.flowType = flowType;
    this.prm = prm;
    this.genericParameterType = genericParameterType;
    this.type = type;
    this.name = StringUtils.defaultIfBlank(name, prm.getName());
    this.fieldName = fieldName;
    this.nullableAnnotation = nullableAnnotation;
  }
}
