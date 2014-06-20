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
