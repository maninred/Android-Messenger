package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static String myPort="";
	static String myPort2="";
	String beforePort="";
	String afterPort="";
	String myPortHashed="";
	List<String> allPorts=new ArrayList<String>();
	List<String> allPortsDoubled=new ArrayList<String>();
	List<String> allPortsHashed=new ArrayList<String>();
	String[] allPortsSorted;
	String[] allPortsSortedHashed;
	//Set<String> nodesFailed=new LinkedHashSet<String>();
	String returnedValue="";
	List<ContentValues> allContentValues=new ArrayList<ContentValues>();
	List<ContentValues> ContentValuesMissed=new ArrayList<ContentValues>();
	boolean queryCheck=false;
	boolean firstPass=false;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Context context=getContext();
		File f = context.getFilesDir();
		File file[] = f.listFiles();
		List <File> filenames=new ArrayList<File>();
		for(File AllFiles:file) {
			String[] filesSplits=AllFiles.toString().split("/");
			String lastName=filesSplits[filesSplits.length-1];
			if(lastName.trim().equals(selection))filenames.add(AllFiles);
			if(selection.equals("*")) filenames.add(AllFiles);
			if(selection.equals("@")) filenames.add(AllFiles);
			Log.i("AllFiles",filesSplits[filesSplits.length-1]);
		}
		for(File AllFiles:filenames) {
			AllFiles.delete();
			Log.i("delete","deleting "+AllFiles.toString());
		}


		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


		//Log.i("nodesFailed","**************************"+nodesFailed.toString());
		String FILENAME = values.get("key").toString();
		String string = values.get("value").toString().trim();
		FileOutputStream outputStream;
		Context context=getContext();
		boolean onlyhere=false;
		try {
			Log.i("insert","Entered");
			String portFind= null;
			if(allContentValues.contains(values)){
				onlyhere=true;
				portFind=myPort;
				allContentValues.remove(values);
			}
			else {
				try {
					portFind = findingPort(FILENAME);
				} catch (NoSuchAlgorithmException e) {
					Log.e("insert", "findingPort exception");
				}
			}
			Log.i("insert","Found port.........!");
			if(!portFind.equals(myPort)){
				Log.i("Port To be Sent",portFind);
				String doubleNodes[]=nextTwo(portFind);
				ClientTask justNaming =new ClientTask();
				String msg=FILENAME+"&"+string+"&"+(Integer.parseInt(portFind)*2);
				justNaming.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort2);
//				justNaming.get();
				for(String port:doubleNodes){
					String msg1=FILENAME+"&"+string+"&"+(Integer.parseInt(port)*2);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, myPort2);
					//justNamingOut.get();
				}
				return uri;
			}
			else if(!FILENAME.equals("PING")){
				outputStream = context.openFileOutput(FILENAME, Context.MODE_PRIVATE);
				Log.i("insert", "output stream of : " + FILENAME);
				outputStream.write(string.getBytes());
				outputStream.close();
				if(!onlyhere) {
					String doubleNodes[]=nextTwo(portFind);
					for (String port : doubleNodes) {
//					String msg = FILENAME + "&" + string + "&" + (Integer.parseInt(doubleNodes[0]) * 2)+ "&" + (Integer.parseInt(doubleNodes[1]) * 2);
						String msg = FILENAME + "&" + string + "&" + (Integer.parseInt(port) * 2);
						ClientTask justNaming1 =new ClientTask();
						justNaming1.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort2);
					//justNaming.get();
					}
				}
				Log.v("insert", values.toString());
				return uri;
			}
		} catch (Exception e) {
			Log.e("Insert", "Unable to write in file "+FILENAME);
		}
		return uri;
	}

	public String findingPort(String FILENAME) throws NoSuchAlgorithmException {
		String FILENAMEHashed=genHash(FILENAME);
		Log.i("FILENAMEHashed",FILENAMEHashed);
		int tt=0;
		boolean checked=false;
		String outHashed="";

		for(int t=0;t<allPortsSorted.length;t++){
			if(FILENAMEHashed.compareTo(allPortsSortedHashed[t])<0){
				tt=t;
				outHashed=allPortsSorted[t];
				break;
			}
		}
		if(outHashed.length()==0){
			tt=0;
			outHashed=allPortsSorted[0];
		}
		Log.i("portFind",outHashed);
		return outHashed;
	}


	public String[] nextTwo(String portsent){
		String[] doubleNodes=new String[2];
		int j;
		for(j=0;j<allPortsSorted.length;j++){
			if(portsent.equals(allPortsSorted[j])) break;
		}
		doubleNodes[0]=allPortsSorted[(j+1)%5];
		doubleNodes[1]=allPortsSorted[(j+2)%5];
		Log.i("double","------------------------------------------"+doubleNodes[0]);
		Log.i("double","------------------------------------------"+doubleNodes[1]);
		return doubleNodes;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		allPorts.addAll(Arrays.asList("5554","5556","5558","5560","5562"));
		allPortsDoubled.addAll(Arrays.asList("11108","11112","11116","11120","11124"));
		allPortsSorted=new String[allPorts.size()];
		allPortsSortedHashed=new String[allPorts.size()];

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portStr));
		myPort2=String.valueOf(Integer.parseInt(portStr)*2);

		int i=0;
		for(String temp:allPorts){
			try {
				allPortsHashed.add(genHash(temp));
				allPortsSortedHashed[i++]=allPortsHashed.get(i-1);
				if(temp.equals(myPort)) myPortHashed=allPortsHashed.get(i-1);
			}
			catch (NoSuchAlgorithmException e) {
				Log.i("genHash","No such Algos Exception");
			}
		}
		Arrays.sort(allPortsSortedHashed);
		for(int t=0;t<allPortsSortedHashed.length;t++){
			int ind=allPortsHashed.indexOf(allPortsSortedHashed[t]);
			allPortsSorted[t]=allPorts.get(ind);
			Log.i("allPortsSorted[t]",allPortsSorted[t]);
			Log.i("allPortsSortedHashed[t]",allPortsSortedHashed[t]);
		}

		for(int t=0;t<allPortsSorted.length;t++){
			if(myPort.equals(allPortsSorted[t])){
				beforePort=allPortsSorted[((t+allPortsSorted.length)-1)%allPortsSorted.length];
				afterPort=allPortsSorted[((t+allPortsSorted.length)+1)%allPortsSorted.length];
				break;
			}
		}
		Log.i("At Port",myPort);

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		for(String portsOther: allPortsDoubled){
			if(!myPort2.equals(portsOther)){
				String msg="PING&PING&"+portsOther;
				ClientTask t=new ClientTask();
				t.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort2);
				try {
					t.get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		firstPass=true;


		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket socket;
			boolean check = true;
			while(check) {
				try {
					socket = serverSocket.accept();
					InputStream serverInputStream = socket.getInputStream();
					InputStreamReader serverISR = new InputStreamReader(serverInputStream);
					BufferedReader serverBR = new BufferedReader(serverISR);
					String msg = serverBR.readLine();
					Log.i("ServerTask",""+msg);
					String[] keyValue = msg.split("&");
					Uri.Builder uriBuilder = new Uri.Builder();
					uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
					String reverseMessage="";

					ContentValues contentValue = new ContentValues();
					if(keyValue[1].equals("QUERY")) {
						if(firstPass) {
							Cursor resultCursor = query(uriBuilder.build(), null, keyValue[0], null, null);
							if (keyValue[0].equals("@")) {
								if (resultCursor.getCount() != 0) {
									do {
										String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
										String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
										reverseMessage += key + "&" + value + " ";
									} while (resultCursor.moveToNext());
								}
							} else {
								reverseMessage = resultCursor.getString(resultCursor.getColumnIndex("value"));
								Log.i("QUERY PROCESSED", "" + reverseMessage);
							}
						}
						else{
							reverseMessage="";
						}
					}
					else if(keyValue[0].equals("PING")) {
						if(ContentValuesMissed.size()!=0) {
							reverseMessage="PING&PING ";
							for (ContentValues temp : ContentValuesMissed) {
								String key = temp.get("key").toString();
								String value = temp.get("value").toString().trim();
								reverseMessage += key + "&" + value + " ";
							}
						}
						else{
							reverseMessage="";
						}
						Log.i("PING", "IN SERVER");
						ContentValuesMissed.clear();
					}
					else {
						contentValue.put("key", keyValue[0]);
						contentValue.put("value", keyValue[1]);
						allContentValues.add(contentValue);
						insert(uriBuilder.build(), contentValue);
						Log.i(TAG, "Message received at server of port " + myPort + " :" + msg);
						reverseMessage="received";
					}
					//Output stream of server task
					OutputStream serverOutputStream = socket.getOutputStream();
					OutputStreamWriter serverOSW = new OutputStreamWriter(serverOutputStream);
					BufferedWriter serverBW = new BufferedWriter(serverOSW);
					serverBW.write(reverseMessage+ "\n");
					serverBW.flush();
					socket.close();
					Log.i(TAG, "Server socket is closed");
				} catch (IOException e) {
					Log.i("server","IO exception Server task!");
				}
			}
			return null;
		}
	}



	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			Log.i("ClientTask","Started");
			String msgToSend = msgs[0].trim();
			String[] msgSplit=msgToSend.split("&");
			msgToSend=msgSplit[0]+"&"+msgSplit[1];
			for(int i=2;i<msgSplit.length;i++) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgSplit[i].trim()));
					Log.i("ClientTaskSocket", "Client Task socket created!");
					boolean a = true;
					while (a) {
						//output stream of client task
						OutputStream clientOutputStream = socket.getOutputStream();
						OutputStreamWriter clientOSW = new OutputStreamWriter(clientOutputStream);
						BufferedWriter clientBW = new BufferedWriter(clientOSW);
						clientBW.write(msgToSend + "\n");
						clientBW.flush();

						//input stream of client task
						InputStream clientInputStream = socket.getInputStream();
						InputStreamReader clientISR = new InputStreamReader(clientInputStream);
						BufferedReader clientBR = new BufferedReader(clientISR);
						String message = clientBR.readLine();
						if (message == null && msgToSend.equals(msgSplit[0] + "&QUERY")) {
							Log.i("Query", "Node failed");
							//nodesFailed.add(msgSplit[2]);
							socket.close();
							break;
						} else if (message == null && msgSplit[0].equals("PING")) {
							Log.i("PING", "Client Null ");
							socket.close();
							break;
						} else if (message == null && msgSplit[1].equals("QUERY")) {
							queryCheck = true;
							socket.close();
							break;
						} else if (message == null && !msgSplit[0].equals("PING")) {
							//nodeFailed=""+(Integer.parseInt(msgSplit[i])/2);
							//nodesFailed.add(msgSplit[2]);
							Uri.Builder uriBuilder = new Uri.Builder();
							uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
							ContentValues contentValue = new ContentValues();
							contentValue.put("key", msgSplit[0]);
							contentValue.put("value", msgSplit[1]);
							//insert(uriBuilder.build(), contentValue);
							ContentValuesMissed.add(contentValue);
							Log.i("Null", "Client Null " + msgSplit[i]);
							socket.close();
							break;
						} else if (message.contains("PING&PING")) {
							message = message.trim();
							String[] firstLevel = message.split(" ");
							for (String inn : firstLevel) {
								if (inn.equals("PING&PING")) continue;
								String[] cons = inn.split("&");
								Uri.Builder uriBuilder = new Uri.Builder();
								uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
								ContentValues contentValue = new ContentValues();
								contentValue.put("key", cons[0]);
								contentValue.put("value", cons[1]);
								insert(uriBuilder.build(), contentValue);
							}
							socket.close();
							Log.i("PING", "At client");
							break;
						} else if (message.equals("received")) {
							Log.i(TAG, "the message sent by port " + msgs[1] + " is received !!!");
							socket.close();
							Log.i(TAG, "client socket is closed");
							break;
						} else {
							Log.i("client", "query value recieved!!");
							returnedValue = message.trim();
							Log.i("client", "" + returnedValue);
							socket.close();
							break;
						}
						//break;
					}
				} catch (Exception e) {
					Log.e(TAG, "ClientTask socket IOException");
					ContentValues contentValue = new ContentValues();
					contentValue.put("key", msgSplit[0]);
					contentValue.put("value", msgSplit[1]);
					//insert(uriBuilder.build(), contentValue);
					ContentValuesMissed.add(contentValue);
					//nodesFailed.add(msgSplit[2]);
					return null;
				}
			}
			return null;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		String[] keyValue={"key","value"};
		MatrixCursor queryMatrixCursor= new MatrixCursor(keyValue);
		Context context=getContext();
		FileInputStream fileInputStream;
		String value="";
		File f = context.getFilesDir();
		File file[] = f.listFiles();
		List <String> filenames=new ArrayList<String>();
		for(File AllFiles:file) {
			String[] filesSplits=AllFiles.toString().split("/");
			filenames.add(filesSplits[filesSplits.length-1]);
			Log.i("AllFiles",""+filesSplits[filesSplits.length-1]);
		}
		Log.i("query",f.toString());
		if(selection.equals("*") || selection.equals("@")) {
			for(File files:file) {
				String[] filesSplit=files.toString().split("/");
				Log.i("query",files.toString());
				try {
					fileInputStream = context.openFileInput(filesSplit[filesSplit.length-1]);
					InputStreamReader queryInputStreamReader = new InputStreamReader(fileInputStream);
					BufferedReader queryBufferedReader = new BufferedReader(queryInputStreamReader);
					value = queryBufferedReader.readLine().toString().trim();
					Log.i("query", "Value for the key " + selection + " is : " + value);
				} catch (FileNotFoundException e) {
					Log.e(TAG, "File not found error while querying");
				} catch (IOException e) {
					Log.e(TAG, "IO error while querying");
				}
				Log.i("query", "At query!!!");
				if(value.length()!=0)
					queryMatrixCursor.newRow().add(filesSplit[filesSplit.length-1]).add(value);
				Log.v("query", filesSplit[filesSplit.length-1] + " " + value);
			}

			if(selection.equals("*")){
				for(String eachPort:allPortsDoubled){
					if(!eachPort.equals(myPort2) ){
						String VeryLongString=FindReturnValue("@",String.valueOf(Integer.parseInt(eachPort)/2)).trim();
						if(VeryLongString.length()==0) continue;
						String[] lines=VeryLongString.split(" ");
						for(String line:lines){
							String[] kvTemp=line.trim().split("&");
							queryMatrixCursor.newRow().add(kvTemp[0]).add(kvTemp[1]);
						}
					}
				}
			}
		}
		else if(filenames.contains(selection)){
			try {
				fileInputStream = context.openFileInput(selection);
				InputStreamReader queryInputStreamReader = new InputStreamReader(fileInputStream);
				BufferedReader queryBufferedReader = new BufferedReader(queryInputStreamReader);
				value = queryBufferedReader.readLine().toString().trim();
				Log.i("query", "Value for the key " + selection + " is : " + value);

			} catch (FileNotFoundException e) {
				Log.e(TAG, "File not found error while querying");
			} catch (Exception e) {
				Log.e(TAG, "error while querying");
			}
			Log.i("query", "At query!!!");
			queryMatrixCursor.newRow().add(selection).add(value);
			Log.v("query", selection + " " + value);
		}
		else{
			Log.i("Query","Passed to another emulator");
			String tempResult="";
			try {
				String portFind= findingPort(selection);
				//if(!nodeFailed.equals(portFind))
				tempResult=FindReturnValue(selection,portFind);
				String doubleNodes[]=nextTwo(portFind);
				int tt=0;
				while(tempResult.length()==0){
					Log.i("query","Next one");
					//nodesFailed.add(String.valueOf(Integer.parseInt(portFind)*2));
					Log.i("Null","Query Null ");
					tempResult=FindReturnValue(selection,doubleNodes[tt++]);
				}
				Uri.Builder uriBuilder = new Uri.Builder();
				uriBuilder.scheme("content").authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
				queryMatrixCursor.newRow().add(selection).add(tempResult);
			} catch (NoSuchAlgorithmException e) {
				Log.i("query","No such algorithm");
			}
		}
		queryMatrixCursor.moveToFirst();
		return queryMatrixCursor;
	}

	public String FindReturnValue(String selection,String portFind){
		returnedValue="";
		try {
			String msg=selection+"&"+"QUERY"+"&"+(Integer.parseInt(portFind)*2);
			Log.i("returnedValueBefore",returnedValue);
			ClientTask temp=new ClientTask();
			temp.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort2);
			temp.get();
			Log.i("returnedValueAfter",""+returnedValue);
		}  catch (Exception e) {
			Log.i("query","got error");
		}
		return returnedValue;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
