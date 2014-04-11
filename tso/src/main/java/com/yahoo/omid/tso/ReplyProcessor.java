package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitResponse;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReplyProcessor implements EventHandler<ReplyProcessor.ReplyEvent>
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplyProcessor.class);

    public void onEvent(final ReplyEvent event, final long sequence, final boolean endOfBatch)
        throws Exception
    {
        switch (event.getType()) {
        case COMMIT_QUERY:
            handleCommitQueryReply(event);
            break;
        case COMMIT:
            handleCommitReply(event);
            break;
        default:
            LOG.error("Unknown event {}", event.getType());
            break;
        }
    }

    private void handleCommitQueryReply(ReplyEvent event) {
        Channel channel = event.getContext().getChannel();

        CommitQueryResponse msg = new CommitQueryResponse(event.getStartTimestamp());
        msg.queryTimestamp = event.getQueryTimestamp();
        if (event.getCommitTimestamp() != 0) {
            msg.commitTimestamp = event.getCommitTimestamp();
            msg.committed = event.getCommitted();
        } else if (event.getRetry()) {
            msg.retry = true;
        } else {
            msg.committed = false;
        }
        event.getContext().getChannel().write(msg);
    }

    private void handleCommitReply(ReplyEvent event) {
        Channel channel = event.getContext().getChannel();

        CommitResponse msg = new CommitResponse(event.getStartTimestamp());
        msg.committed = event.getCommitted();
        msg.commitTimestamp = event.getCommitTimestamp();

        event.getContext().getChannel().write(msg);
    }
    
    public final static class ReplyEvent {
        enum Type {
            COMMIT_QUERY, COMMIT
        }
        private Type type = null;
        private ChannelHandlerContext ctx = null;

        private long startTimestamp = 0;
        private long commitTimestamp = 0;
        private long queryTimestamp = 0;
        private boolean committed = false;
        private boolean retry = true;

       Type getType() { return type; }
       ReplyEvent setType(Type type) {
           this.type = type;
           return this;
       }
       long getStartTimestamp() { return startTimestamp; }
       ReplyEvent setStartTimestamp(long startTimestamp) {
           this.startTimestamp = startTimestamp;
           return this;
       }
       long getCommitTimestamp() { return commitTimestamp; }
       ReplyEvent setCommitTimestamp(long commitTimestamp) {
           this.commitTimestamp = commitTimestamp;
           return this;
       }
       long getQueryTimestamp() { return queryTimestamp; }
       ReplyEvent setQueryTimestamp(long queryTimestamp) {
           this.queryTimestamp = queryTimestamp;
           return this;
       }
       boolean getCommitted() { return committed; }
       ReplyEvent setCommitted(boolean committed) {
           this.committed = committed;
           return this;
       }
       boolean getRetry() { return retry; }
       ReplyEvent setRetry(boolean retry) {
           this.retry = retry;
           return this;
       }
       ChannelHandlerContext getContext() { return ctx; }
       ReplyEvent setContext(ChannelHandlerContext ctx) {
           this.ctx = ctx;
           return this;
       }

       public final static EventFactory<ReplyEvent> EVENT_FACTORY
           = new EventFactory<ReplyEvent>()
       {
           public ReplyEvent newInstance()
           {
               return new ReplyEvent();
           }
       };
   }
}

