package com.solace.connectors.tracing.internal;

import java.util.Optional;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.runtime.extension.api.error.MuleErrors;

/** Error types: SOLACE-TRACING:PUBLISH, :CONSUMING, :TIMEOUT, :ACK and :CONNECTIVITY. */
public enum SolaceError implements ErrorTypeDefinition<SolaceError> {

    PUBLISH,
    CONSUMING,
    TIMEOUT,
    ACK,
    CONNECTIVITY(MuleErrors.CONNECTIVITY);

    private ErrorTypeDefinition<? extends Enum<?>> parent;

    SolaceError() {
    }

    SolaceError(ErrorTypeDefinition<? extends Enum<?>> parent) {
        this.parent = parent;
    }

    @Override
    public Optional<ErrorTypeDefinition<? extends Enum<?>>> getParent() {
        return Optional.ofNullable(parent);
    }
}
