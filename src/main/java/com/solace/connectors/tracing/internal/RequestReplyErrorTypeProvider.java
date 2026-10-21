package com.solace.connectors.tracing.internal;

import java.util.HashSet;
import java.util.Set;

import org.mule.runtime.extension.api.annotation.error.ErrorTypeProvider;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

/** Errors of the Publish consume (request-reply) operation. */
public class RequestReplyErrorTypeProvider implements ErrorTypeProvider {

    @Override
    public Set<ErrorTypeDefinition> getErrorTypes() {
        Set<ErrorTypeDefinition> errors = new HashSet<ErrorTypeDefinition>();
        errors.add(SolaceError.PUBLISH);
        errors.add(SolaceError.CONSUMING);
        errors.add(SolaceError.TIMEOUT);
        errors.add(SolaceError.CONNECTIVITY);
        return errors;
    }
}
