/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.jasn1;

import com.beanit.asn1bean.ber.types.BerType;
import org.apache.nifi.logging.ComponentLog;

import java.io.*;

import org.apache.nifi.util.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nifi.jasn1.JASN1Utils.invokeGetter;

public class StreamingRecordModelIteratorProvider implements RecordModelIteratorProvider {

    public static final int LOOKAHEAD_READLIMIT = 16384;

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<BerType> iterator(InputStream inputStream, ComponentLog logger, Class<? extends BerType> rootClass, String recordField, Field seqOfField) {
        if (StringUtils.isEmpty(recordField)) {
            return new Iterator<BerType>() {
                final BufferedInputStream buffInputStr
                        = new BufferedInputStream(
                        inputStream);
                final BerType model;
                {
                    try {
                            model = rootClass.getDeclaredConstructor().newInstance();
                        } catch (ReflectiveOperationException e) {
                            throw new RuntimeException("Failed to instantiate " + rootClass.getCanonicalName(), e);
                        }
                    if (buffInputStr.markSupported() != true) {
                        throw new RuntimeException("Failed to initialize ASN decoding buffer.Change StreamingRecordModelIteratorProvider ! " + rootClass.getCanonicalName());
                    }
                }
                final AtomicBoolean lookahead = new AtomicBoolean(false);
                @Override
                public boolean hasNext() {
                    lookahead.set(false);
                    buffInputStr.mark(LOOKAHEAD_READLIMIT);

                    try {
                        final int decode = model.decode(buffInputStr);
                        lookahead.set(true);
                        buffInputStr.mark(LOOKAHEAD_READLIMIT);
                        logger.debug("Decoded {} bytes into {}", new Object[]{decode, model.getClass()});
                    } catch (InterruptedIOException e){
                        logger.debug("Interrupted while reading. re-throwing {} as SocketTimeoutException", e.getClass());
                        throw new RuntimeException( new SocketTimeoutException(e.getMessage()));
                                     // interrupted while blocking during read, normal behaviour
                                    // throwing a RuntimeException is wrong though, as it's intercepted by Framework and generates noise in the Log
                    } catch (IOException e) {
                        // the implemetation of SSL in NIFI  org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel
                        // SSL can mask a EOFException in an javax.net.ssl.SSLException which is also a java.io.IOException
                        if (e instanceof EOFException || e.getCause() instanceof EOFException) {
                            logger.debug("EOF while reading {}", model.getClass());
                            try {
                                buffInputStr.close();
                                inputStream.close();
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                            return false; // End hit because Stream closed
                        } else /* catch (IOException e) */ {
                            throw new RuntimeException("Failed to decode " + rootClass.getCanonicalName(), e);
                        }
                    }
                    try {
                        buffInputStr.reset();
                    }finally {
                        return lookahead.get();
                    }
                }

                @Override
                public BerType next() {
                    if (lookahead.get()) {
                        return model;
                    } else if(hasNext()) {
                        return next();
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            };
        } else {
            final BerType model = decode(inputStream, logger, rootClass);

            final List<BerType> recordModels;
            try {
                final Method recordModelGetter = rootClass.getMethod(JASN1Utils.toGetterMethod(recordField));
                final BerType readPointModel = (BerType) recordModelGetter.invoke(model);
                if (seqOfField != null) {
                    final Class seqOf = JASN1Utils.getSeqOfElementType(seqOfField);
                    recordModels = (List<BerType>) invokeGetter(readPointModel, JASN1Utils.toGetterMethod(seqOf.getSimpleName()));
                } else {
                    recordModels = Collections.singletonList(readPointModel);
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to get record models due to " + e, e);
            }

            return recordModels.iterator();
        }
    }

    private BerType decode(InputStream inputStream, ComponentLog logger, Class<? extends BerType> rootClass) {
        final BerType model;

        try {
            model = rootClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate " + rootClass.getCanonicalName(), e);
        }

        try {
            final int decode = model.decode(inputStream);
            logger.debug("Decoded {} bytes into {}", new Object[]{decode, model.getClass()});
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode " + rootClass.getCanonicalName(), e);
        }


        return model;
    }
}
