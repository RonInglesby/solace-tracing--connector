package com.solace.connectors.tracing.internal;

import java.util.Optional;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.runtime.extension.api.error.MuleErrors;

/** Error types raised by the connector: SOLACE-TRACING:PUBLISH and SOLACE-TRACING:CONNECTIVITY. */
public enum SolaceError implements ErrorTypeDefinition<SolaceError> {

    PUBLISH,
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
