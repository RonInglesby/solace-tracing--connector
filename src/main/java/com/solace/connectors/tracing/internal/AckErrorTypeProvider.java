package com.solace.connectors.tracing.internal;

import java.util.HashSet;
import java.util.Set;

import org.mule.runtime.extension.api.annotation.error.ErrorTypeProvider;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

/** Errors of the Ack and Recover session operations. */
public class AckErrorTypeProvider implements ErrorTypeProvider {

    @Override
    public Set<ErrorTypeDefinition> getErrorTypes() {
        Set<ErrorTypeDefinition> errors = new HashSet<ErrorTypeDefinition>();
        errors.add(SolaceError.ACK);
        return errors;
    }
}
