package io.grpc.examples;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.Random;

public class ErrorInjectingInterceptor implements ServerInterceptor {

    private final Random random = new Random();

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {

        boolean fail = random.nextBoolean();
        ServerCall.Listener<ReqT> lstnr = next.startCall(call, headers);
        if (fail) {
            lstnr = new ErrorInjectingListener<>(lstnr, call, Status.UNAVAILABLE);
        }
        return lstnr;
    }
}
