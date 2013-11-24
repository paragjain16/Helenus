package org.ds.socket;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.ds.logger.DSLogger;

public class DSocket {

	private InputStream in;
	private OutputStream out;
	private Socket socket;

	public DSocket(String hostName, int port) throws UnknownHostException,
			IOException {
		socket = new Socket(hostName, port);
		in = socket.getInputStream();
		out = socket.getOutputStream();
	}

	public DSocket(Socket s) throws UnknownHostException, IOException {
		socket = s;
		out = s.getOutputStream();
		in = s.getInputStream();
	}

	public InputStream getIn() {
		return in;
	}

	public void setIn(InputStream in) {
		this.in = in;
	}

	public OutputStream getOut() {
		return out;
	}

	public void setOut(OutputStream out) {
		this.out = out;
	}

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public int read() throws IOException {
		return in.read();
	}

	public int read(byte[] b) throws IOException {
		return in.read(b);
	}

	public String readLine() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		return br.readLine();
	}

	public void write(byte[] b) throws IOException {
		// System.out.println("writing"+b.toString());
		out.write(b);
		out.flush();
	}

	public void write(String string) throws IOException {
		DataOutputStream dso = new DataOutputStream(this.out);
		dso.writeUTF(string);
		dso.flush();
	}

	public String readString() throws IOException {
		DataInputStream dis = new DataInputStream(in);
		// System.out.println("reading");
		String s = dis.readUTF();
		// System.out.println(s);
		return s;
		// return dis.readUTF();
	}

	public void close() throws IOException {
		in.close();
		out.close();
		socket.close();
	}

	public void writeMultipleLines(List<String> strList) throws IOException {
		PrintWriter pwOutputStream = null;
		DSLogger.log("DSocket", "writeMultiple", "Entering");
		pwOutputStream = new PrintWriter(out);
		for (String str : strList) {
			DSLogger.log("DSocket", "writeMultiple", "Wrote: " + str);
			pwOutputStream.println(str);
		}
		pwOutputStream.flush();
		DSLogger.log("DSocket", "writeMultiple", "Exiting");
	}

	public List<String> readMultipleLines() throws IOException {
		BufferedReader inputBuffer = null;
		DSLogger.log("DSocket", "readMultiple", "Entering");
		inputBuffer = new BufferedReader(new InputStreamReader(in));

		List<String> strList = new ArrayList<String>();
		String str = null;

		while (true) {
			str = inputBuffer.readLine();
			DSLogger.log("DSocket", "readMultiple", "Read String: " + str);
			if (str.contains("end~!#!")) {
				break;
			}

			strList.add(str);
			// System.out.println("Read:"+str);
		}
		DSLogger.log("DSocket", "readMultiple", "Exiting");
		return strList;

	}

	public Object readObject() throws IOException {
		DSLogger.log("DSocket", "readObject", "Entering");
		ObjectInputStream ois = new ObjectInputStream(in);
		Object o = null;
		try {
			o = ois.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		DSLogger.log("DSocket", "readMultiple", "Exiting");
		return o;

	}

	public void writeObject(Object obj) throws IOException {
		DSLogger.log("DSocket", "writeObject", "Entering");
		ObjectOutputStream objOutStream = null;
		try {
			objOutStream = new ObjectOutputStream(out);
			objOutStream.writeObject(obj);
			objOutStream.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
		DSLogger.log("DSocket", "writeObject", "Exiting");
	}

	public void writeObjectList(List<Object> objList) throws IOException {
		DSLogger.log("DSocket", "writeObjectList", "Entering");
		ObjectOutputStream objOutStream = null;
		try {
			objOutStream = new ObjectOutputStream(out);
			objOutStream.writeObject(objList);
			
			objOutStream.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
		DSLogger.log("DSocket", "writeObject", "Exiting");
	}
}
