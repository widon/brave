package brave.internal.recorder;

import brave.Span.Kind;
import brave.Tracer;
import brave.internal.IpLiteral;
import brave.internal.Nullable;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Locale;

/**
 * This represents a span except for its {@link TraceContext}. It is mutable, for late adjustments.
 *
 * <p>While in-flight, the data is synchronized where necessary. When exposed to users, it can be
 * mutated without synchronization.
 */
public final class MutableSpan implements Cloneable {

  public interface TagConsumer<T> {
    void accept(T target, String key, String value);
  }

  public interface AnnotationConsumer<T> {
    void accept(T target, long timestamp, String value);
  }

  /*
   * One of these objects is allocated for each in-flight span, so we try to be parsimonious on things
   * like array allocation and object reference size.
   */
  Kind kind;
  boolean shared;
  long startTimestamp, finishTimestamp;
  String name, remoteServiceName, remoteIp;
  int remotePort;

  /**
   * To reduce the amount of allocation, collocate annotations with tags in a pair-indexed list.
   * This will be (startTimestamp, value) for annotations and (key, value) for tags.
   */
  final ArrayList<Object> pairs;

  public MutableSpan() {
    pairs = new ArrayList<>(6); // assume 3 tags and no annotations
  }

  MutableSpan(MutableSpan source) {
    kind = source.kind;
    shared = source.shared;
    startTimestamp = source.startTimestamp;
    finishTimestamp = source.finishTimestamp;
    name = source.name;
    remoteServiceName = source.remoteServiceName;
    remoteIp = source.remoteIp;
    remotePort = source.remotePort;
    pairs = (ArrayList<Object>) source.pairs.clone();
  }

  /** Returns the {@link brave.Span#name(String) span name} or null */
  @Nullable public String name() {
    return name;
  }

  /** @see brave.Span#name(String) */
  public void name(String name) {
    if (name == null) throw new NullPointerException("name == null");
    this.name = name;
  }

  /** Returns the {@link brave.Span#start(long) span start timestamp} or zero */
  public long startTimestamp() {
    return startTimestamp;
  }

  /** @see brave.Span#start(long) */
  public void startTimestamp(long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  /** Returns the {@link brave.Span#finish(long) span finish timestamp} or zero */
  public long finishTimestamp() {
    return finishTimestamp;
  }

  /** @see brave.Span#finish(long) */
  public void finishTimestamp(long finishTimestamp) {
    this.finishTimestamp = finishTimestamp;
  }

  /** Returns the {@link brave.Span#kind(brave.Span.Kind) span kind} or null */
  public Kind kind() {
    return kind;
  }

  /** @see brave.Span#kind(brave.Span.Kind) */
  public void kind(Kind kind) {
    if (kind == null) throw new NullPointerException("kind == null");
    this.kind = kind;
  }

  /** @see brave.Span#remoteServiceName(String) */
  @Nullable public String remoteServiceName() {
    return remoteServiceName;
  }

  /** @see brave.Span#remoteServiceName(String) */
  public void remoteServiceName(String remoteServiceName) {
    if (remoteServiceName == null || remoteServiceName.isEmpty()) {
      throw new NullPointerException("remoteServiceName is empty");
    }
    this.remoteServiceName = remoteServiceName.toLowerCase(Locale.ROOT);
  }

  /**
   * The text representation of the primary IPv4 or IPv6 address associated with the remote side of
   * this connection. Ex. 192.168.99.100 null if unknown.
   *
   * @see brave.Span#remoteIpAndPort(String, int)
   */
  @Nullable public String remoteIp() {
    return remoteIp;
  }

  /**
   * Port of the remote IP's socket or 0, if not known.
   *
   * @see java.net.InetSocketAddress#getPort()
   * @see brave.Span#remoteIpAndPort(String, int)
   */
  public int remotePort() {
    return remotePort;
  }

  /** @see brave.Span#remoteIpAndPort(String, int) */
  public boolean remoteIpAndPort(@Nullable String remoteIp, int remotePort) {
    if (remoteIp == null) return false;
    this.remoteIp = IpLiteral.ipOrNull(remoteIp);
    if (this.remoteIp == null) return false;
    if (remotePort > 0xffff) throw new IllegalArgumentException("invalid port " + remotePort);
    if (remotePort < 0) remotePort = 0;
    this.remotePort = remotePort;
    return true;
  }

  /** @see brave.Span#annotate(String) */
  public void annotate(long timestamp, String value) {
    if (value == null) throw new NullPointerException("value == null");
    if (timestamp == 0L) return;
    pairs.add(timestamp);
    pairs.add(value);
  }

  /** @see brave.Span#tag(String, String) */
  public void tag(String key, String value) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key is empty");
    if (value == null) throw new NullPointerException("value == null");
    for (int i = 0, length = pairs.size(); i < length; i += 2) {
      if (key.equals(pairs.get(i))) {
        pairs.set(i + 1, value);
        return;
      }
    }
    pairs.add(key);
    pairs.add(value);
  }

  public <T> void forEachTag(TagConsumer<T> tagConsumer, T target) {
    for (int i = 0, length = pairs.size(); i < length; i += 2) {
      Object first = pairs.get(i);
      if (first instanceof Long) continue; // hit an annotation
      tagConsumer.accept(target, first.toString(), pairs.get(i + 1).toString());
    }
  }

  public <T> void forEachAnnotation(AnnotationConsumer<T> annotationConsumer, T target) {
    for (int i = 0, length = pairs.size(); i < length; i += 2) {
      Object first = pairs.get(i);
      if (first instanceof String) continue; // hit a tag
      annotationConsumer.accept(target, (Long) first, pairs.get(i + 1).toString());
    }
  }

  /** Returns true if the span ID is {@link #setShared() shared} with a remote client. */
  public boolean shared() {
    return shared;
  }

  /**
   * Indicates we are contributing to a span started by another tracer (ex on a different host).
   * Defaults to false.
   *
   * @see Tracer#joinSpan(TraceContext)
   * @see zipkin2.Span#shared()
   */
  public void setShared() {
    shared = true;
  }

  /** Returns a result which modifies the data without affecting the source. */
  @Override public MutableSpan clone() {
    return new MutableSpan(this);
  }
}
