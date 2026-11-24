package com.flower.anno.flow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Flow state field. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface State {
  /**
   * Flow state field name. If not specified, reflection will be used to get a corresponding
   * parameter name.
   *
   * @return Flow state field name
   */
  String name() default "";

  /**
   * Whether the state field is constant. The corresponding state class field must be final.
   * Constant fields can't be used as Out and InOut parameters.
   *
   * @return True, if the state field is constant.
   */
  boolean isConst() default false;

  /**
   * Declares that the state field is always initialized (non-null) when the Flow or EventProfile
   * instance is created, e.g. by a field initializer ({@code @State(initialized = true) Integer x = 0;})
   * or by every constructor. Such fields are treated as initialized by Flower's null-safety check,
   * like final fields. The declaration is verified when the Flow starts (or the EventProfile instance
   * is created): if the field is null, the Flow fails.
   *
   * <p>Not needed for final fields and fields of primitive types, which are always treated as
   * initialized. Can't be combined with @Nullable.
   *
   * @return True, if the state field is initialized on creation.
   */
  boolean initialized() default false;
}
