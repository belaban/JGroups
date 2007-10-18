package org.jgroups.demos;

import java.awt.Frame;
import java.awt.Label;
import java.awt.Rectangle;
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;

import javax.swing.JButton;

public class Chat extends ChatCore implements MouseListener, WindowListener {

	Frame mainFrame;

	TextArea txtArea;

	TextField txtField;

	Label csLabel;

	JButton leaveButton;

	JButton sendButton;

	JButton clearButton;

	public Chat(String props) {
		super(props);
	}

	public static void main(String[] args) {
		String props = null;

		for (int i = 0; i < args.length; i++) {
			if ("-props".equals(args[i])) {
				props = args[++i];
				continue;
			}
			help();
			return;
		}

		Chat instance = new Chat(props);
		instance.start();
	}

	void post(String msg) {
		txtArea.append(msg);
	}

	static void help() {
		System.out.println("Chat [-help] [-props <properties>]");
	}

	public void start() {
		mainFrame = new Frame();
		mainFrame.setLayout(null);
		mainFrame.setSize(600, 507);
		mainFrame.addWindowListener(this);

		txtArea = new TextArea();
		txtArea.setBounds(12, 36, 550, 348);
		txtArea.setEditable(false);
		mainFrame.add(txtArea);

		txtField = new TextField();
		txtField.setBounds(100, 392, 400, 30);
		mainFrame.add(txtField);

		csLabel = new Label("Send:");
		csLabel.setBounds(12, 392, 85, 30);
		mainFrame.add(csLabel);

		leaveButton = new JButton("Leave");
		leaveButton.setBounds(12, 428, 150, 30);
		leaveButton.addMouseListener(this);
		mainFrame.add(leaveButton);

		sendButton = new JButton("Send");
		sendButton.setBounds(182, 428, 150, 30);
		sendButton.addMouseListener(this);
		mainFrame.add(sendButton);

		clearButton = new JButton("Clear");
		clearButton.setBounds(340, 428, 150, 30);
		clearButton.addMouseListener(this);
		mainFrame.add(clearButton);

		link();

		mainFrame.pack();
		mainFrame.setLocation(15, 25);
		mainFrame.setBounds(new Rectangle(580, 480));
		mainFrame.setVisible(true);
		mainFrame.show();

		dumpHist();

	}

	public void mouseClicked(MouseEvent e) {
		Object obj = e.getSource();

		if (obj == leaveButton) {
			handleLeave();
			System.exit(0);
		} else if (obj == sendButton)
			handleSend(txtField.getText());
		else if (obj == clearButton)
			txtArea.setText("");
	}

	public void mouseEntered(MouseEvent e) {
	}

	public void mouseExited(MouseEvent e) {
	}

	public void mousePressed(MouseEvent e) {
	}

	public void mouseReleased(MouseEvent e) {
	}

	public void windowActivated(WindowEvent e) {
	}

	public void windowClosed(WindowEvent e) {
	}

	public void windowClosing(WindowEvent e) {
		handleLeave();
		System.exit(0);
	}

	public void windowDeactivated(WindowEvent e) {
	}

	public void windowDeiconified(WindowEvent e) {
	}

	public void windowIconified(WindowEvent e) {
	}

	public void windowOpened(WindowEvent e) {
	}

}
