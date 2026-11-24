package com.flower.engine.configuration;

import com.flower.anno.event.EventType;
import com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.annotation.AnnotationFormatError;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class FunctionRecord {
  List<FunctionParameterRecord> loadGlobalFunctionSignature(Method method) {
    return loadFunctionSignature(null, method, FunctionType.GLOBAL, null);
  }

  List<FunctionParameterRecord> loadEventFunctionSignature(Method method, EventType[] eventTypes) {
    return loadFunctionSignature(null, method, FunctionType.EVENT, eventTypes);
  }

  List<FunctionParameterRecord> loadFunctionSignature(
      Class<?> flowType, Method method, FunctionType functionType) {
    return loadFunctionSignature(flowType, method, functionType, null);
  }

  FunctionReturnValueRecord loadFunctionReturnValue(@Nullable Class<?> flowType, Method method) {
    Type genericReturnValueType = method.getGenericReturnType();
    return new FunctionReturnValueRecord(
        flowType, genericReturnValueType, method.getAnnotation(Nullable.class) != null);
  }

  /**
   * Asynchronous functions must return {@link CompletableFuture}; any other future type (e.g.
   * Guava ListenableFuture) would otherwise be treated as a plain synchronous return value.
   */
  static void validateAsyncReturnType(Method method) {
    Class<?> returnType = method.getReturnType();
    if ((Future.class.isAssignableFrom(returnType)
            || CompletionStage.class.isAssignableFrom(returnType))
        && !CompletableFuture.class.isAssignableFrom(returnType)) {
      throw new IllegalStateException(
          String.format(
              "Function return type [%s] is not supported: asynchronous functions must return [%s]. Function Method [%s]",
              returnType.getName(), CompletableFuture.class.getName(), method));
    }
  }

  private List<FunctionParameterRecord> loadFunctionSignature(
      @Nullable Class<?> flowType,
      Method method,
      FunctionType functionType,
      @Nullable EventType[] eventTypes) {
    if (eventTypes == null) eventTypes = new EventType[] {};
    validateAsyncReturnType(method);

    List<FunctionParameterRecord> parameters = new ArrayList<>();
    String physicalFunctionName =
        String.format("%s.%s", method.getDeclaringClass().getCanonicalName(), method.getName());

    Type[] genericParameterTypes = method.getGenericParameterTypes();
    Parameter[] params = method.getParameters();

    FunctionParameterRecord inRetFound = null, intRetOrExceptionFound = null;

    Map<String, FunctionParameterRecord> parameterMap = new HashMap<>();
    for (int i = 0; i < params.length; i++) {
      Parameter prm = params[i];
      Type genericParameterType = genericParameterTypes[i];
      FunctionParameterRecord parameter =
          loadParameter(
              flowType, prm, genericParameterType, physicalFunctionName, functionType, eventTypes);
      parameters.add(parameter);

      if (parameter.type.equals(FunctionParameterType.IN_RET)) inRetFound = parameter;
      if (parameter.type.equals(FunctionParameterType.IN_RET_OR_EXCEPTION))
        intRetOrExceptionFound = parameter;

      if (!parameterMap.containsKey(parameter.name)) parameterMap.put(parameter.name, parameter);
      else
        throw new IllegalStateException(
            "Conflicting ParameterNames: name ["
                + parameter.name
                + "] Function Method ["
                + method
                + "]");
    }

    // The reason why we can't have both @InRet and @InRetOrException, is because in cases when an
    // Exception is raised
    // the value of @InRet is undefined. For that reason we can only have @InRet in cases when
    // Exception interrupts
    // execution on Flower level, while @InRetOrException allows for the flow to proceed.
    if (inRetFound != null && intRetOrExceptionFound != null)
      throw new IllegalStateException(
          "Function can't have both IN_RET and IN_RET_OR_EXCEPTION parameters: IN_RET name ["
              + inRetFound.name
              + "] IN_RET_OR_EXCEPTION name ["
              + intRetOrExceptionFound.name
              + "] Function Method ["
              + method
              + "]");

    return parameters;
  }

  FunctionParameterRecord loadParameter(
      @Nullable Class<?> flowType,
      Parameter prm,
      Type genericParameterType,
      String physicalFunctionName,
      FunctionType functionType,
      @Nullable EventType[] eventTypes) {
    // Only one Flower Parameter annotation is allowed
    ParameterAnnotationSpec<?> spec = null;
    Annotation first = null;
    for (ParameterAnnotationSpec<?> candidate : ParameterAnnotationSpec.ALL) {
      Annotation annotation = candidate.get(prm);
      if (annotation != null) {
        if (first == null) {
          first = annotation;
          spec = candidate;
        } else {
          throw new AnnotationFormatError(
              String.format(
                  "Function %s. Parameter %s is annotated as both: %s, %s.",
                  physicalFunctionName, prm.getName(), first, annotation));
        }
      }
    }

    Nullable nullableAnnotation = prm.getAnnotation(Nullable.class);
    if (nullableAnnotation != null && (spec == null || !spec.nullableAllowed)) {
      throw new AnnotationFormatError(
          String.format(
              "Function %s. Parameter %s cannot be annotated as @Nullable. Only @In, @InRet and @InFromFlow parameters can be Nullable.",
              physicalFunctionName, prm.getName()));
    }

    if (spec == null) {
      // Flower Parameter annotation is required
      throw new AnnotationFormatError(
          String.format(
              "Function %s. Parameter %s should be annotated as one of [@In, @Out, @InOut, @Exec, @EventBus, @StepRef, @Terminal, @InRet, @InRetOrException, @FlowFactory, @FlowRepo, @EventInfo, @FlowInfo, @StepInfo, @TransitionInfo, @FlowException]",
              physicalFunctionName, prm.getName()));
    }

    validateParameter(
        spec, prm, physicalFunctionName, functionType, Preconditions.checkNotNull(eventTypes));
    return createRecord(spec, prm, nullableAnnotation, genericParameterType, flowType);
  }

  static <A extends Annotation> FunctionParameterRecord createRecord(
      ParameterAnnotationSpec<A> spec,
      Parameter prm,
      @Nullable Nullable nullableAnnotation,
      Type genericParameterType,
      @Nullable Class<?> flowType) {
    A annotation = Preconditions.checkNotNull(spec.get(prm));
    return spec.createRecord(annotation, nullableAnnotation, prm, genericParameterType, flowType);
  }

  void validateParameter(
      ParameterAnnotationSpec<?> spec,
      Parameter prm,
      String physicalFunctionName,
      FunctionType functionType,
      EventType[] eventTypes) {
    if (spec.nullableRequired && prm.getAnnotation(Nullable.class) == null) {
      throw new AnnotationFormatError(
          String.format(
              "Function %s. Parameter %s annotated as %s must be @Nullable",
              physicalFunctionName, prm.getName(), spec.displayName));
    }
    if (spec.checkOrder == ParameterAnnotationSpec.CheckOrder.JAVA_TYPE_FIRST) {
      validateJavaType(spec, prm, physicalFunctionName);
      validateFunctionType(spec, prm, physicalFunctionName, functionType);
    } else {
      validateFunctionType(spec, prm, physicalFunctionName, functionType);
      validateJavaType(spec, prm, physicalFunctionName);
    }
    if (spec.allowedEventTypes != null
        && notAllEventTypesSupported(spec.allowedEventTypes, eventTypes)) {
      throw new AnnotationFormatError(
          String.format(
              "Function %s. Parameter %s annotated as %s can only be used in EventFunction for the following EventTypes %s",
              physicalFunctionName, prm.getName(), spec.displayName, spec.allowedEventTypes));
    }
  }

  static void validateFunctionType(
      ParameterAnnotationSpec<?> spec,
      Parameter prm,
      String physicalFunctionName,
      FunctionType functionType) {
    if (spec.allowedFunctionTypes != null && !spec.allowedFunctionTypes.contains(functionType)) {
      throw new AnnotationFormatError(
          String.format(
              "Function %s. Parameter %s annotated as %s can only be used in %s",
              physicalFunctionName,
              prm.getName(),
              spec.displayName,
              spec.allowedFunctionTypesDescription));
    }
  }

  static void validateJavaType(
      ParameterAnnotationSpec<?> spec, Parameter prm, String physicalFunctionName) {
    if (spec.allowedJavaTypes != null && !spec.allowedJavaTypes.contains(prm.getType())) {
      throw new AnnotationFormatError(
          String.format(
              "Function %s. Parameter %s annotated as %s should be of type %s",
              physicalFunctionName,
              prm.getName(),
              spec.displayName,
              spec.allowedJavaTypes.stream()
                  .map(Class::getCanonicalName)
                  .collect(Collectors.joining(" or "))));
    }
  }

  boolean notAllEventTypesSupported(
      Set<EventType> allowedEventTypes, EventType[] eventTypesToCheck) {
    for (EventType eventType : eventTypesToCheck) {
      if (!allowedEventTypes.contains(eventType)) return true;
    }
    return false;
  }
}
