package brave;

import brave.internal.IpLiteral;
import brave.internal.Nullable;
import brave.internal.Platform;
import brave.internal.recorder.PendingSpans;
import brave.internal.recorder.SpanReporter;
import brave.propagation.B3Propagation;
import brave.propagation.CurrentTraceContext;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.sampler.Sampler;
import java.io.Closeable;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.Sender;

/**
 * This provides utilities needed for trace instrumentation. For example, a {@link Tracer}.
 *
 * <p>Instances built via {@link #newBuilder()} are registered automatically such that statically
 * configured instrumentation like JDBC drivers can use {@link #current()}.
 *
 * <p>This type can be extended so that the object graph can be built differently or overridden,
 * for example via spring or when mocking.
 */
public abstract class Tracing implements Closeable {

  public static Builder newBuilder() {
    return new Builder();
  }

  /** All tracing commands start with a {@link Span}. Use a tracer to create spans. */
  abstract public Tracer tracer();

  /**
   * When a trace leaves the process, it needs to be propagated, usually via headers. This utility
   * is used to inject or extract a trace context from remote requests.
   */
  // Implementations should override and cache this as a field.
  public Propagation<String> propagation() {
    return propagationFactory().create(Propagation.KeyFactory.STRING);
  }

  /** This supports edge cases like GRPC Metadata propagation which doesn't use String keys. */
  abstract public Propagation.Factory propagationFactory();

  /**
   * Sampler is responsible for deciding if a particular trace should be "sampled", i.e. whether the
   * overhead of tracing will occur and/or if a trace will be reported to Zipkin.
   *
   * @see Tracer#withSampler(Sampler)
   */
  abstract public Sampler sampler();

  /**
   * This supports in-process propagation, typically across thread boundaries. This includes
   * utilities for concurrent types like {@linkplain java.util.concurrent.ExecutorService}.
   */
  abstract public CurrentTraceContext currentTraceContext();

  /**
   * This exposes the microsecond clock used by operations such as {@link Span#finish()}. This is
   * helpful when you want to time things manually. Notably, this clock will be coherent for all
   * child spans in this trace (that use this tracing component). For example, NTP or system clock
   * changes will not affect the result.
   *
   * @param context references a potentially unstarted span you'd like a clock correlated with
   */
  public final Clock clock(TraceContext context) {
    return tracer().pendingSpans.getOrCreate(context).clock();
  }

  abstract public ErrorParser errorParser();

  // volatile for visibility on get. writes guarded by Tracing.class
  static volatile Tracing current = null;

  /**
   * Returns the most recently created tracer if its component hasn't been closed. null otherwise.
   *
   * <p>This object should not be cached.
   */
  @Nullable public static Tracer currentTracer() {
    Tracing tracing = current;
    return tracing != null ? tracing.tracer() : null;
  }

  final AtomicBoolean noop = new AtomicBoolean(false);

  /**
   * When true, no recording is done and nothing is reported to zipkin. However, trace context is
   * still injected into outgoing requests.
   *
   * @see Span#isNoop()
   */
  public boolean isNoop() {
    return noop.get();
  }

  /**
   * Set true to drop data and only return {@link Span#isNoop() noop spans} regardless of sampling
   * policy. This allows operators to stop tracing in risk scenarios.
   *
   * @see #isNoop()
   */
  public void setNoop(boolean noop) {
    this.noop.set(noop);
  }

  /**
   * Returns the most recently created tracing component iff it hasn't been closed. null otherwise.
   *
   * <p>This object should not be cached.
   */
  @Nullable public static Tracing current() {
    return current;
  }

  /** Ensures this component can be garbage collected, by making it not {@link #current()} */
  @Override abstract public void close();

  public static final class Builder {
    String localServiceName = "unknown", localIp;
    int localPort; // zero means null
    Reporter<zipkin2.Span> reporter;
    Clock clock;
    Sampler sampler = Sampler.ALWAYS_SAMPLE;
    CurrentTraceContext currentTraceContext = CurrentTraceContext.Default.inheritable();
    boolean traceId128Bit = false;
    boolean supportsJoin = true;
    Propagation.Factory propagationFactory = B3Propagation.FACTORY;
    ErrorParser errorParser = new ErrorParser();

    /**
     * Lower-case label of the remote node in the service graph, such as "favstar". Avoid names with
     * variables or unique identifiers embedded. Defaults to "unknown".
     *
     * <p>This is a primary label for trace lookup and aggregation, so it should be intuitive and
     * consistent. Many use a name from service discovery.
     *
     * @see #localIp(String)
     */
    public Builder localServiceName(String localServiceName) {
      if (localServiceName == null || localServiceName.isEmpty()) {
        throw new IllegalArgumentException(localServiceName + " is not a valid serviceName");
      }
      this.localServiceName = localServiceName.toLowerCase(Locale.ROOT);
      return this;
    }

    /**
     * The text representation of the primary IP address associated with this service. Ex.
     * 192.168.99.100 or 2001:db8::c001. Defaults to a link local IP.
     *
     * @see #localServiceName(String)
     * @see #localPort(int)
     * @since 5.2
     */
    public Builder localIp(String localIp) {
      String maybeIp = IpLiteral.ipOrNull(localIp);
      if (maybeIp == null) throw new IllegalArgumentException(localIp + " is not a valid IP");
      this.localIp = maybeIp;
      return this;
    }

    /**
     * The primary listen port associated with this service. No default.
     *
     * @see #localIp(String)
     * @since 5.2
     */
    public Builder localPort(int localPort) {
      if (localPort > 0xffff) throw new IllegalArgumentException("invalid localPort " + localPort);
      if (localPort < 0) localPort = 0;
      this.localPort = localPort;
      return this;
    }

    /**
     * Sets the {@link zipkin2.Span#localEndpoint Endpoint of the local service} being traced.
     *
     * @deprecated Use {@link #localServiceName(String)} {@link #localIp(String)} and {@link
     * #localPort(int)}. Will be removed in Brave v6.
     */
    @Deprecated
    public Builder endpoint(zipkin2.Endpoint endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.localServiceName = endpoint.serviceName();
      this.localIp = endpoint.ipv6() != null ? endpoint.ipv6() : endpoint.ipv4();
      this.localPort = endpoint.portAsInt();
      return this;
    }

    /**
     * Controls how spans are reported. Defaults to logging, but often an {@link AsyncReporter}
     * which batches spans before sending to Zipkin.
     *
     * The {@link AsyncReporter} includes a {@link Sender}, which is a driver for transports like
     * http, kafka and scribe.
     *
     * <p>For example, here's how to batch send spans via http:
     *
     * <pre>{@code
     * spanReporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
     *
     * tracingBuilder.spanReporter(spanReporter);
     * }</pre>
     *
     * <p>See https://github.com/openzipkin/zipkin-reporter-java
     */
    public Builder spanReporter(Reporter<zipkin2.Span> reporter) {
      if (reporter == null) throw new NullPointerException("spanReporter == null");
      this.reporter = reporter;
      return this;
    }

    /**
     * Assigns microsecond-resolution timestamp source for operations like {@link Span#start()}.
     * Defaults to JRE-specific platform time.
     *
     * <p>Note: timestamps are read once per trace, then {@link System#nanoTime() ticks}
     * thereafter. This ensures there's no clock skew problems inside a single trace.
     *
     * See {@link Tracing#clock(TraceContext)}
     */
    public Builder clock(Clock clock) {
      if (clock == null) throw new NullPointerException("clock == null");
      this.clock = clock;
      return this;
    }

    /**
     * Sampler is responsible for deciding if a particular trace should be "sampled", i.e. whether
     * the overhead of tracing will occur and/or if a trace will be reported to Zipkin.
     *
     * @see Tracer#withSampler(Sampler) for temporary overrides
     */
    public Builder sampler(Sampler sampler) {
      if (sampler == null) throw new NullPointerException("sampler == null");
      this.sampler = sampler;
      return this;
    }

    /**
     * Responsible for implementing {@link Tracer#startScopedSpan(String)}, {@link
     * Tracer#currentSpanCustomizer()}, {@link Tracer#currentSpan()} and {@link
     * Tracer#withSpanInScope(Span)}.
     *
     * <p>By default a simple thread-local is used. Override to support other mechanisms or to
     * synchronize with other mechanisms such as SLF4J's MDC.
     */
    public Builder currentTraceContext(CurrentTraceContext currentTraceContext) {
      if (currentTraceContext == null) {
        throw new NullPointerException("currentTraceContext == null");
      }
      this.currentTraceContext = currentTraceContext;
      return this;
    }

    /**
     * Controls how trace contexts are injected or extracted from remote requests, such as from http
     * headers. Defaults to {@link B3Propagation#FACTORY}
     */
    public Builder propagationFactory(Propagation.Factory propagationFactory) {
      if (propagationFactory == null) throw new NullPointerException("propagationFactory == null");
      this.propagationFactory = propagationFactory;
      return this;
    }

    /** When true, new root spans will have 128-bit trace IDs. Defaults to false (64-bit) */
    public Builder traceId128Bit(boolean traceId128Bit) {
      this.traceId128Bit = traceId128Bit;
      return this;
    }

    /**
     * True means the tracing system supports sharing a span ID between a {@link Span.Kind#CLIENT}
     * and {@link Span.Kind#SERVER} span. Defaults to true.
     *
     * <p>Set this to false when the tracing system requires the opposite. For example, if
     * ultimately spans are sent to Amazon X-Ray or Google Stackdriver Trace, you should set this to
     * false.
     *
     * <p>This is implicitly set to false when {@link Propagation.Factory#supportsJoin()} is false,
     * as in that case, sharing IDs isn't possible anyway.
     *
     * @see Propagation.Factory#supportsJoin()
     */
    public Builder supportsJoin(boolean supportsJoin) {
      this.supportsJoin = supportsJoin;
      return this;
    }

    public Builder errorParser(ErrorParser errorParser) {
      this.errorParser = errorParser;
      return this;
    }

    public Tracing build() {
      if (clock == null) clock = Platform.get().clock();
      if (localIp == null) localIp = Platform.get().linkLocalIp();
      if (reporter == null) reporter = Platform.get().reporter();
      return new Default(this);
    }

    Builder() {
    }
  }

  static final class Default extends Tracing {
    final Tracer tracer;
    final Propagation.Factory propagationFactory;
    final Propagation<String> stringPropagation;
    final CurrentTraceContext currentTraceContext;
    final Sampler sampler;
    final Clock clock;
    final ErrorParser errorParser;

    Default(Builder builder) {
      this.clock = builder.clock;
      this.errorParser = builder.errorParser;
      this.propagationFactory = builder.propagationFactory;
      this.stringPropagation = builder.propagationFactory.create(Propagation.KeyFactory.STRING);
      this.currentTraceContext = builder.currentTraceContext;
      this.sampler = builder.sampler;
      zipkin2.Endpoint localEndpoint = zipkin2.Endpoint.newBuilder()
          .serviceName(builder.localServiceName)
          .ip(builder.localIp)
          .port(builder.localPort)
          .build();
      SpanReporter reporter = new SpanReporter(localEndpoint, builder.reporter, noop);
      this.tracer = new Tracer(
          builder.clock,
          builder.propagationFactory,
          reporter,
          new PendingSpans(localEndpoint, clock, reporter, noop),
          builder.sampler,
          builder.errorParser,
          builder.currentTraceContext,
          builder.traceId128Bit || propagationFactory.requires128BitTraceId(),
          builder.supportsJoin && propagationFactory.supportsJoin(),
          noop
      );
      maybeSetCurrent();
    }

    @Override public Tracer tracer() {
      return tracer;
    }

    @Override public Propagation<String> propagation() {
      return stringPropagation;
    }

    @Override public Propagation.Factory propagationFactory() {
      return propagationFactory;
    }

    @Override public Sampler sampler() {
      return sampler;
    }

    @Override public CurrentTraceContext currentTraceContext() {
      return currentTraceContext;
    }

    @Override public ErrorParser errorParser() {
      return errorParser;
    }

    private void maybeSetCurrent() {
      if (current != null) return;
      synchronized (Tracing.class) {
        if (current == null) current = this;
      }
    }

    @Override public void close() {
      if (current != this) return;
      // don't blindly set most recent to null as there could be a race
      synchronized (Tracing.class) {
        if (current == this) current = null;
      }
    }
  }

  Tracing() { // intentionally hidden constructor
  }
}
