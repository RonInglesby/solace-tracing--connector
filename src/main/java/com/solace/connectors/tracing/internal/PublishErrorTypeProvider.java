package com.solace.connectors.tracing.internal;

import java.util.HashSet;
import java.util.Set;

import org.mule.runtime.extension.api.annotation.error.ErrorTypeProvider;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

/** Declares the errors the publish operation can throw, so flows can handle them in error handlers. */
public class PublishErrorTypeProvider implements ErrorTypeProvider {

    @Override
    public Set<ErrorTypeDefinition> getErrorTypes() {
        Set<ErrorTypeDefinition> errors = new HashSet<ErrorTypeDefinition>();
        errors.add(SolaceError.PUBLISH);
        errors.add(SolaceError.CONNECTIVITY);
        return errors;
    }
}
