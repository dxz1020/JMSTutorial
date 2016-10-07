package cs3219.jms.order;

import java.util.Properties;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Matric 1:A0108453J
 * Name   1:DUAN XUZHOU
 * 
 * Matric 2:A0111217Y
 * Name   2:NG ZHI HUA
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe {
	// your code here
	String customerName;
	String productName;
	private Queue queue;
    private QueueConnectionFactory qconFactory;
    private QueueConnection qcon;
	Boolean status;
	public JmsPipe(String SimpleConnectionFactory, String SimpleQueue) throws Exception {
		// TODO Auto-generated constructor stub
		InitialContext ic = getInitialContext();
		qconFactory = (QueueConnectionFactory) ic.lookup(SimpleConnectionFactory);
		customerName=new String();
		productName=new String();
		status=true;
	}

	@Override
	public void write(Order s) {
		// TODO Auto-generated method stub
		s.setCustomerName(customerName);
		s.setProductName(productName);
		status=false;
	}

	@Override
	public Order read() {
		// TODO Auto-generated method stub
		Order order=new Order(customerName, productName);
		if(status==true){
			return order;
		}
		else{
			return null;
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		status=false;
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
