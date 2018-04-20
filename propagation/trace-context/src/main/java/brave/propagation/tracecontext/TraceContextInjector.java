package brave.propagation.tracecontext;

import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Injector;
import brave.propagation.tracecontext.TraceContextPropagation.Extra;

import static brave.propagation.tracecontext.TraceparentFormat.writeTraceparentFormat;

final class TraceContextInjector<C, K> implements Injector<C> {
  final TracestateFormat tracestateFormat;
  final Setter<C, K> setter;
  final K traceparentKey, tracestateKey;

  TraceContextInjector(TraceContextPropagation<K> propagation, Setter<C, K> setter) {
    this.tracestateFormat = new TracestateFormat(propagation.stateName);
    this.traceparentKey = propagation.traceparentKey;
    this.tracestateKey = propagation.tracestateKey;
    this.setter = setter;
  }

  @Override
  public void inject(TraceContext traceContext, C carrier) {
    String thisState = writeTraceparentFormat(traceContext);
    setter.put(carrier, traceparentKey, thisState);

    CharSequence otherState = null;
    for (int i = 0, length = traceContext.extra().size(); i < length; i++) {
      Object next = traceContext.extra().get(i);
      if (next instanceof Extra) {
        otherState = ((Extra) next).otherState;
        break;
      }
    }

    String tracestate = tracestateFormat.write(thisState, otherState);
    setter.put(carrier, tracestateKey, tracestate);
  }
}
