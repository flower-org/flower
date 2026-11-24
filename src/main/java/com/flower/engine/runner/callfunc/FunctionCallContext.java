package com.flower.engine.runner.callfunc;

import com.flower.engine.function.FunctionCallParameter;
import com.flower.engine.function.ParameterType;

import java.lang.reflect.Method;
import java.util.List;

public class FunctionCallContext {
  public final List<FunctionCallParameter> functionParameters;
  public final Method function;
  public final String functionName;

  public FunctionCallContext(
      List<FunctionCallParameter> functionParameters, Method function, String functionName) {
    this.functionParameters = functionParameters;
    this.function = function;
    this.functionName = functionName;
  }

  public boolean hasParameterOfType(ParameterType parameterType) {
    for (FunctionCallParameter parameter : functionParameters) {
      if (parameter.getFunctionParameterType() == parameterType) {
        return true;
      }
    }
    return false;
  }

  //TODO: Maybe at some point rewire everything and replace this with expected Flow/Step info
  public String getFlowStepInfo() {
    return String.format("{ Class: %s; Method: %s }", function.getDeclaringClass().getName(), function.getName());
  }
}
