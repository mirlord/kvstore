package io.grpc.examples;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;

public class ErrorInjectingListener<ReqT, RespT> extends
        ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

    private final ServerCall<ReqT, RespT> call;

    private boolean closed;

    private final Status status;

    protected ErrorInjectingListener(ServerCall.Listener<ReqT> delegate,
                                     ServerCall<ReqT, RespT> call,
                                     Status status) {
        super(delegate);
        this.call = call;
        this.status = status;
    }

    @Override
    public void onMessage(ReqT message) {
        closeCall();
    }

    @Override
    public void onHalfClose() {
        closeCall();
    }

    @Override
    public void onCancel() {
        closeCall();
    }

    @Override
    public void onComplete() {
        closeCall();
    }

    @Override
    public void onReady() {
        closeCall();
    }

    private void closeCall() {
        if (!closed) {
            try {
                call.close(status, new Metadata());
            } catch (IllegalStateException ise) {
                // ignore
            }
            closed = true;
        }
    }
}
