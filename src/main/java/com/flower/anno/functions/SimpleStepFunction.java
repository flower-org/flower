package com.flower.anno.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A function that combines StepFunction and TransitFunction. Can be referred by name as a
 * StepFunction for transition purposes. Can NOT be referred by name by other StepFunctions as their
 * TransitFunction.
 *
 * <p>Function must be static. Function must return Transit or CompletableFuture&lt;Transit&gt;
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SimpleStepFunction {
  /**
   * Step name. If not specified, reflection will be used to get a corresponding method name.
   *
   * @return Step name
   */
  String name() default "";

  /**
   * Optional: what the Step does, shown under its name in the generated Mermaid diagram.
   *
   * @return Step note
   */
  String note() default "";
}
