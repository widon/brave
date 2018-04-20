package brave.propagation.tracecontext;

import brave.propagation.Propagation.Getter;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.propagation.tracecontext.TraceContextPropagation.Extra;
import java.util.Collections;
import java.util.List;

import static brave.propagation.tracecontext.TraceparentFormat.FORMAT_LENGTH;
import static brave.propagation.tracecontext.TraceparentFormat.maybeExtractParent;
import static brave.propagation.tracecontext.TraceparentFormat.validateFormat;

final class TraceContextExtractor<C, K> implements Extractor<C> {
  final Getter<C, K> getter;
  final K tracestateKey;
  final TracestateFormat tracestateFormat;

  TraceContextExtractor(TraceContextPropagation<K> propagation, Getter<C, K> getter) {
    this.getter = getter;
    this.tracestateKey = propagation.tracestateKey;
    this.tracestateFormat = new TracestateFormat(propagation.stateName);
  }

  @Override
  public TraceContextOrSamplingFlags extract(C carrier) {
    if (carrier == null) throw new NullPointerException("carrier == null");
    String tracestateString = getter.get(carrier, tracestateKey);
    if (tracestateString == null) return EMPTY;

    TraceparentFormatHandler handler = new TraceparentFormatHandler();
    CharSequence otherState = tracestateFormat.parseAndReturnOtherState(tracestateString, handler);

    List<Object> extra;
    if (otherState == null) {
      extra = DEFAULT_EXTRA;
    } else {
      Extra e = new Extra();
      e.otherState = otherState;
      extra = Collections.singletonList(e);
    }

    if (handler.context == null) {
      if (extra == DEFAULT_EXTRA) return EMPTY;
      return TraceContextOrSamplingFlags.newBuilder()
          .extra(extra)
          .samplingFlags(SamplingFlags.EMPTY)
          .build();
    }
    return TraceContextOrSamplingFlags.newBuilder().context(handler.context).extra(extra).build();
  }

  static final class TraceparentFormatHandler implements TracestateFormat.Handler {
    TraceContext context;

    @Override
    public boolean onThisState(CharSequence tracestateString, int pos) {
      if (validateFormat(tracestateString, pos) < FORMAT_LENGTH) {
        return false;
      }
      context = maybeExtractParent(tracestateString, pos);
      return true;
    }
  }

  /** When present, this context was created with TracestatePropagation */
  static final Extra MARKER = new Extra();

  static final List<Object> DEFAULT_EXTRA = Collections.singletonList(MARKER);
  static final TraceContextOrSamplingFlags EMPTY =
      TraceContextOrSamplingFlags.EMPTY.toBuilder().extra(DEFAULT_EXTRA).build();
}
