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
import org.mule.runtime.extension.api.annotation.param.display.Placement;
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

    // ---- Guaranteed message flow control (Studio: "Flow control" tab). Empty = Solace API default.

    @Parameter
    @Optional
    @Placement(tab = FLOW_CONTROL_TAB, order = 1)
    @DisplayName("Receive window size")
    @Summary("Guaranteed message receive window (JCSMP SUB_ACK_WINDOW_SIZE), 1-255. Empty = API default (18).")
    private Integer receiveWindowSize;

    @Parameter
    @Optional
    @Placement(tab = FLOW_CONTROL_TAB, order = 2)
    @DisplayName("Receive ack threshold (%)")
    @Summary("Send the transport ack when this percentage of the window is received (JCSMP SUB_ACK_THRESHOLD), 1-75. Empty = API default (60).")
    private Integer receiveAckThreshold;

    @Parameter
    @Optional
    @Placement(tab = FLOW_CONTROL_TAB, order = 3)
    @DisplayName("Receive ack timer (ms)")
    @Summary("Maximum time before a transport ack is sent (JCSMP SUB_ACK_TIME), 20-1500. Empty = API default (1000).")
    private Integer receiveAckTimerMs;

    private static final String FLOW_CONTROL_TAB = "Flow control";

    @Override
    public SolaceConnection connect() throws ConnectionException {
        checkRange("receiveWindowSize", receiveWindowSize, 1, 255);
        checkRange("receiveAckThreshold", receiveAckThreshold, 1, 75);
        checkRange("receiveAckTimerMs", receiveAckTimerMs, 20, 1500);

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
            if (receiveWindowSize != null) {
                cf.setReceiveADWindowSize(receiveWindowSize);
            }
            if (receiveAckThreshold != null) {
                cf.setReceiveAdAckThreshold(receiveAckThreshold);
            }
            if (receiveAckTimerMs != null) {
                cf.setReceiveADAckTimerInMillis(receiveAckTimerMs);
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

    /** Fails fast with a clear message instead of a vague Solace API error. */
    private static void checkRange(String name, Integer value, int min, int max) throws ConnectionException {
        if (value != null && (value < min || value > max)) {
            throw new ConnectionException(name + " must be between " + min + " and " + max + " (was " + value + ")");
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
