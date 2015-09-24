package ws.wamp.jawampa;

public class MessageInfo<T> {
	
	
	T message;
	String topic;
	
	public MessageInfo(T msg, String topic){
		this.message = msg;
		this.topic = topic;
	}

	public T getMessage() {
		return message;
	}
	public void setMessage(T message) {
		this.message = message;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	
	
}
