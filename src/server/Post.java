package server;

public class Post {
    private String sender, recipient, post;

    public Post(String sender, String recipient, String post) {
        this.sender = sender;
        this.post = post;
        this.recipient = recipient;
    }

    public String getSender() {
        return sender;
    }

    public String getRecipient() {
        return recipient;
    }

    public String getPost() {
        return post;
    }
}
