/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.yahoo.omid.tso.PersistenceProcessor;
import com.yahoo.omid.tso.PersistenceProcessorImpl;
import com.yahoo.omid.tso.ReplyProcessor;
import com.yahoo.omid.tso.ReplyProcessorImpl;
import com.yahoo.omid.tso.RequestProcessor;
import com.yahoo.omid.tso.RetryProcessor;
import com.yahoo.omid.tso.RetryProcessorImpl;

public class DisruptorModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(RequestProcessor.class).to(RequestProcessorImpl.class).in(Singleton.class);
        bind(PersistenceProcessor.class).to(PersistenceProcessorImpl.class).in(Singleton.class);
        bind(ReplyProcessor.class).to(ReplyProcessorImpl.class).in(Singleton.class);
        bind(RetryProcessor.class).to(RetryProcessorImpl.class).in(Singleton.class);

    }

}
