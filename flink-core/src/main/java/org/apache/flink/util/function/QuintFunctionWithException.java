/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.function;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.ExceptionUtils;

/**
 * Function which takes five arguments.
 *
 * @param <S> type of the first argument
 * @param <T> type of the second argument
 * @param <U> type of the third argument
 * @param <V> type of the four argument
 * @param <W> type of the five argument
 * @param <R> type of the return value
 * @param <E> type of the thrown exception
 */
@PublicEvolving
@FunctionalInterface
public interface QuintFunctionWithException<S, T, U, V, W, R, E extends Throwable> {

    /**
     * Applies this function to the given arguments.
     *
     * @param s the first function argument
     * @param t the second function argument
     * @param u the third function argument
     * @param v the four function argument
     * @param w the five function argument
     * @return the function result
     * @throws E if it fails
     */
    R apply(S s, T t, U u, V v, W w) throws E;

    /**
     * Convert at {@link QuintFunctionWithException} into a {@link QuintFunction}.
     *
     * @param quintFunctionWithException function with exception to convert into a function
     * @param <A> first input type
     * @param <B> second input type
     * @param <C> third input type
     * @param <D> fourth input type
     * @param <E> fifth input type
     * @param <R> output type
     * @return {@link QuintFunction} which throws all checked exception as an unchecked exception.
     */
    static <A, B, C, D, E, R> QuintFunction<A, B, C, D, E, R> unchecked(
            QuintFunctionWithException<A, B, C, D, E, R, ?> quintFunctionWithException) {
        return (A a, B b, C c, D d, E e) -> {
            try {
                return quintFunctionWithException.apply(a, b, c, d, e);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
                // we need this to appease the compiler :-(
                return null;
            }
        };
    }
}
