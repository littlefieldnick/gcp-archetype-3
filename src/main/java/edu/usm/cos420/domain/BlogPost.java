package edu.usm.cos420.domain;

public class BlogPost {
	private String title;
	private String author;
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	private String description;
	
	public String toString() {
		return "Title: "
				+  this.title + "\nAuthor: " + this.author + "\nDesciption: " + this.description;
	}

}
