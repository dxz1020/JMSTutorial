package cs3219.jms.order;

import java.util.Properties;

import javax.jms.JMSException;
//import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

//import org.jboss.util.Sync;

/**
 * Matric 1:A0111217Y
 * Name   1:Ng Zhi Hua
 * 
 * Matric 2:A0108453J
 * Name   2:Duan XuZhou
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe {
    
    private QueueConnectionFactory qconFactory;
    private QueueConnection qcon;
    private QueueSession qsession;
    private QueueSender qsender;
    private QueueReceiver qreceiver;
    private Queue queue;
    private TextMessage msg;
    
    JmsPipe(String connectionName, String topicName){
    	try {
    		InitialContext ic = getInitialContext();
			init(ic, topicName, connectionName);

			System.out.println("Starting JMS...");
    	} catch (NamingException e) {
			e.printStackTrace();
		} catch (JMSException e) {
			e.printStackTrace();
		}
    }

	@Override
	public void write(Order s) {
		try{
		msg.setText(s.toString());
		qsender.send(msg);
		}catch(Exception e){
			System.out.println("Error!: " + e.getMessage());
		}
		// TODO Auto-generated method stub
		System.out.println("writing: " + s.toString());
		
	}

	@Override
	public Order read() {
		try {
			TextMessage msg = (TextMessage)qreceiver.receive();
			System.out.println("reading: " + Order.fromString(msg.getText()).toString());
			return Order.fromString(msg.getText());
		} catch (JMSException e) {
			e.printStackTrace();
		}
		
		
		return null;
	}

	@Override
	public void close() {
        try {
        	qreceiver.close();
            qsender.close();
			qsession.close();
	        qcon.close();
		} catch (JMSException e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
    public void init(Context ctx, String queueName, String jmsFactory)
            throws NamingException, JMSException {
        qconFactory = (QueueConnectionFactory) ctx.lookup(jmsFactory);
        qcon = qconFactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) ctx.lookup(queueName);
        qsender = qsession.createSender(queue);
        qreceiver = qsession.createReceiver(queue);
        msg = qsession.createTextMessage();
//        qreceiver.setMessageListener((MessageListener) this);
        qcon.start();
    }
    
    public void initReceiver(Context ctx, String queueName, String jmsFactory)
            throws NamingException, JMSException {
        qconFactory = (QueueConnectionFactory) ctx.lookup(jmsFactory);
        qcon = qconFactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) ctx.lookup(queueName);
        qreceiver = qsession.createReceiver(queue);
//        qreceiver.setMessageListener(this);
        qcon.start();
    }
	
    private static InitialContext getInitialContext()
            throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        return new InitialContext(props);
    }
    
}
