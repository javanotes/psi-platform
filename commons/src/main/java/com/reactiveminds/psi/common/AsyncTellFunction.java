package com.reactiveminds.psi.common;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface AsyncTellFunction extends Function<String, CompletionStage<Long>> {
}
