package com.solace.connectors.tracing.internal;

import javax.jms.Connection;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;

import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.param.display.Summary;

/**
 * Connects with the Solace JMS API directly (no JNDI). The Solace API retries and reconnects on its own;
 * only when its retries are exhausted does Mule's reconnection strategy take over.
 */
@Alias("connection")
public class SolaceConnectionProvider implements CachedConnectionProvider<SolaceConnection> {

    @Parameter
    @Summary("e.g. tcp://broker:55555 or tcps://broker:55443 (comma-separate hosts for failover)")
    private String host;

    @Parameter
    @DisplayName("Message VPN")
    private String vpn;

    @Parameter
    private String username;

    @Parameter
    @Password
    private String password;

    @Parameter
    @Optional(defaultValue = "3")
    @Summary("Initial connect attempts made by the Solace API")
    private int connectRetries;

    @Parameter
    @Optional(defaultValue = "20")
    @Summary("Reconnect attempts made by the Solace API after a connection drops (-1 = forever)")
    private int reconnectRetries;

    @Parameter
    @Optional(defaultValue = "3000")
    @DisplayName("Reconnect retry wait (ms)")
    private int reconnectRetryWaitMs;

    @Parameter
    @Optional(defaultValue = "true")
    @Summary("Validate the broker's TLS certificate (tcps only)")
    private boolean validateCertificate;

    @Parameter
    @Optional
    @Summary("Trust store path for tcps connections")
    private String trustStore;

    @Parameter
    @Optional
    @Password
    private String trustStorePassword;

    @Override
    public SolaceConnection connect() throws ConnectionException {
        Connection jms = null;
        try {
            SolConnectionFactory cf = SolJmsUtility.createConnectionFactory();
            cf.setHost(host);
            cf.setVPN(vpn);
            cf.setUsername(username);
            cf.setPassword(password);
            cf.setConnectRetries(connectRetries);
            cf.setReconnectRetries(reconnectRetries);
            cf.setReconnectRetryWaitInMillis(reconnectRetryWaitMs);
            cf.setSSLValidateCertificate(validateCertificate);
            if (trustStore != null && !trustStore.isEmpty()) {
                cf.setSSLTrustStore(trustStore);
                if (trustStorePassword != null) {
                    cf.setSSLTrustStorePassword(trustStorePassword);
                }
            }
            jms = cf.createConnection();
            return new SolaceConnection(jms);
        } catch (Exception e) {
            if (jms != null) {
                try {
                    jms.close();
                } catch (Exception ignore) {
                    // already failing
                }
            }
            throw new ConnectionException(
                    "Could not connect to Solace at " + host + " (VPN " + vpn + "): " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect(SolaceConnection connection) {
        connection.close();
    }

    @Override
    public ConnectionValidationResult validate(SolaceConnection connection) {
        return connection.isValid()
                ? ConnectionValidationResult.success()
                : ConnectionValidationResult.failure("Solace connection is no longer valid",
                        new ConnectionException("Solace connection lost"));
    }
}
