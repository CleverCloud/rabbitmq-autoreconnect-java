package com.clevercloud.annotations;

import javax.annotation.meta.TypeQualifier;
import javax.annotation.meta.TypeQualifierValidator;
import javax.annotation.meta.When;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Collection;

/**
 * @author Marc-Antoine Perennou<Marc-Antoine@Perennou.com>
 */

@Documented
@TypeQualifier
@Retention(RetentionPolicy.RUNTIME)
public @interface NonEmpty {
   When when() default When.ALWAYS;

   static class Checker implements TypeQualifierValidator<NonEmpty> {

      public When forConstantValue(NonEmpty qualifierqualifierArgument,
                                   Object value) {
         if (value != null && value instanceof Collection && ((Collection) value).isEmpty())
            return When.NEVER;
         return When.ALWAYS;
      }
   }
}
