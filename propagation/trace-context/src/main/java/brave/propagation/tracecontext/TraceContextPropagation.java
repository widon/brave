package brave.propagation.tracecontext;

import brave.propagation.Propagation;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import java.util.Arrays;
import java.util.List;

public final class TraceContextPropagation<K> implements Propagation<K> {

  public static final Propagation.Factory FACTORY =
      new Propagation.Factory() {
        @Override
        public <K> Propagation<K> create(KeyFactory<K> keyFactory) {
          return new TraceContextPropagation<>(keyFactory);
        }

        @Override
        public boolean requires128BitTraceId() {
          return true;
        }

        @Override
        public String toString() {
          return "TracestatePropagationFactory";
        }
      };

  final String stateName;
  final K traceparentKey, tracestateKey;
  final List<K> fields;

  TraceContextPropagation(KeyFactory<K> keyFactory) {
    this.stateName = "tc";
    this.traceparentKey = keyFactory.create("traceparent");
    this.tracestateKey = keyFactory.create("tracestate");
    this.fields = Arrays.asList(traceparentKey, tracestateKey);
  }

  @Override
  public List<K> keys() {
    return fields;
  }

  @Override
  public <C> Injector<C> injector(Setter<C, K> setter) {
    if (setter == null) throw new NullPointerException("setter == null");
    return new TraceContextInjector<>(this, setter);
  }

  @Override
  public <C> Extractor<C> extractor(Getter<C, K> getter) {
    if (getter == null) throw new NullPointerException("getter == null");
    return new TraceContextExtractor<>(this, getter);
  }

  static final class Extra { // hidden intentionally
    CharSequence otherState;

    @Override
    public String toString() {
      return "TracestatePropagation{"
          + (otherState != null ? ("fields=" + otherState.toString()) : "")
          + "}";
    }
  }
}
