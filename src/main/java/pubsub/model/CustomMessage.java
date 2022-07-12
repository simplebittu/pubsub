package pubsub.model;

import java.util.Objects;

public class CustomMessage {

	private String data;
	private boolean isSuccess;
	private String appId;
	
	public CustomMessage() {
		super();
	}
	public CustomMessage(String data, boolean isSuccess, String appId) {
		super();
		this.data = data;
		this.isSuccess = isSuccess;
		this.appId = appId;
	}
	
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(appId, data, isSuccess);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomMessage other = (CustomMessage) obj;
		return Objects.equals(appId, other.appId) && Objects.equals(data, other.data) && isSuccess == other.isSuccess;
	}
	
	
	
}
