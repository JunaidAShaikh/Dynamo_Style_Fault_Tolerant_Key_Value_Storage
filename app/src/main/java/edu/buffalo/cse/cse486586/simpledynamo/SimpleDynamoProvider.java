package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;


public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String[] REMOTE_PORTS_INT = {"5554", "5556", "5558", "5560", "5562"};
    static final int SERVER_PORT = 10000;
    static String myPort, myId, myPortString;
    static int myPortInt;
    static boolean allClear = false;
    static Map<Integer, Boolean> ready = new HashMap<Integer, Boolean>();
    static ArrayList<String> ringPort = new ArrayList<String>();
    static ArrayList<String> ringId = new ArrayList<String>();
    static int staticQueryCount = 0;
    static Map<String, MatrixCursor> mCursorMap = new HashMap<String, MatrixCursor>();
    //static MatrixCursor mCursor;
    //static int insert=0;
    static Map<String, String> resultMap = new HashMap<String, String>();
    static boolean onCreate = false;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    //static int queries = 1, readyCount=1;
    Map<String, String> database = new HashMap<String, String>();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int deleteCount = staticQueryCount++;
        while(true) {
            if(!onCreate)
                break;
        }
        /*while(true)
        {
            if(!onCreate)
                break;
        }*/

        ready.put(deleteCount, false);
        //Log.d(TAG, "Putting deleteCount: "+deleteCount+" = false");
        String key = selection;
        boolean lekeChupBaithJa = false;
        if (key.contains(".")) {
            lekeChupBaithJa = true;
            key = key.replace(".", "");
        }
        String hashKey = null;
        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (selection.equals("*")) {
            if (!allClear) {
                database.clear();
                allClear = true;
                forward('F', 'D', key, ringPort.get((ringPort.indexOf(myPortString) + 1) % 5), deleteCount);
            }
        } else if (selection.equals("@")) {
            database.clear();
            ready.put(deleteCount,true);
            //Log.d(TAG, "Putting deleteCount: "+deleteCount+" = true");
        }
        else {
            if (lekeChupBaithJa) {
                database.remove(key);
                ready.put(deleteCount,true);
                //Log.d(TAG, "Putting deleteCount: "+deleteCount+" = true");
            } else {
                String intendedPort = getIntendedPort(hashKey);

                if (intendedPort.equals(myPortString)) {
                    database.remove(intendedPort + key);
                    forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(myPortString) + 1) % 5), deleteCount);
                    forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(myPortString) + 2) % 5), deleteCount);
                    //forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(myPortString) + 2) % 5));
                } else {
                    forward('F', 'D', "." + intendedPort + key, intendedPort, deleteCount);
                    forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(intendedPort) + 1) % 5), deleteCount);
                    forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(intendedPort) + 2) % 5), deleteCount);
                    //forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(intendedPort) + 1) % 5));
                    //forward('F', 'D', "." + intendedPort + key, ringPort.get((ringPort.indexOf(intendedPort) + 2) % 5));
                }

            }
        }
        while(true) {
            try {
                if (ready.get(deleteCount)) {
                    //Log.d(TAG, "Done, removing deleteCount: " + deleteCount);
                    //ready.remove(deleteCount);
                    break;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                //Log.d(TAG, "Count not find ready for deleteCount: "+deleteCount);
            }
        }
        return 0;

    }

    private void forward(char operation, char subOperation, String msg, String sendTo, int count) {
        //String[] send = sendTo.split("-");
        //for(int i=0; i<send.length; i++)
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, operation + ":" + subOperation + ":" + msg + ":" + sendTo+":"+count, myPort);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        int insertCount = staticQueryCount++;
        while(true) {
            if(!onCreate)
                break;
        }
        ////Log.d(TAG, "recieved insert: "+values.get("key").toString());
        /*while(true)
        {
            if(!onCreate)
                break;
        }*/
        ////Log.d(TAG, "insert allowed: "+values.get("key").toString());
        ready.put(insertCount,false);
        //Log.d(TAG, "Putting insertCount: "+insertCount+" = false");

        String key = values.get("key").toString();
        //Log.d(TAG,"Inserting: "+key);
        boolean lekeChupBaithJa = false;
        if (key.contains(".")) {
            lekeChupBaithJa = true;
            key = key.replace(".", "");
        }
        String hashKey = null;
        String value = values.get("value").toString();
        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (lekeChupBaithJa) {
            if (database.get(key) != null) {
                int version = Integer.parseInt(database.get(key).substring(0, 1)) + 1;
                database.put(key, version + value);
            } else
                database.put(key, "1" + value);
            ready.put(insertCount,true);
            //Log.d(TAG, "Putting insertCount: "+insertCount+" = true");
        } else {
            String intendedPort = getIntendedPort(hashKey);
            if (intendedPort.equals(myPortString)) {
                if (database.get(key) != null) {
                    int version = Integer.parseInt(database.get(key).substring(0, 1)) + 1;
                    database.put(intendedPort + key, version + value);
                } else
                    database.put(intendedPort + key, "1" + value);
                forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(myPortString) + 1) % 5), insertCount);
                forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(myPortString) + 2) % 5), insertCount);
                //forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(myPortString) + 2) % 5));
            } else {
                forward('F', 'I', "." + intendedPort + key + "," + value, intendedPort, insertCount);
                forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(intendedPort) + 1) % 5), insertCount);
                forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(intendedPort) + 2) % 5), insertCount);
                //forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(intendedPort) + 1) % 5));
                //forward('F', 'I', "." + intendedPort + key + "," + value, ringPort.get((ringPort.indexOf(intendedPort) + 2) % 5));
            }

        }
        while (true)
        {
            try {
                if (ready.get(insertCount)) {
                    //Log.d(TAG, "Done, removing insertCount: " + insertCount);
                    //ready.remove(insertCount);
                    break;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                //Log.d(TAG, "Count not find ready for: "+insertCount);
            }
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        Context context = getContext();


        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortString = String.valueOf((Integer.parseInt(portStr)));
        myPortInt = Integer.parseInt(myPort) / 2;
        try {
            myId = genHash(String.valueOf(myPortInt));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket=new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket= new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }

        ringPort.clear();
        ringId.clear();
        ringPort.add("5562");
        ringPort.add("5556");
        ringPort.add("5554");
        ringPort.add("5558");
        ringPort.add("5560");
        ringId.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
        ringId.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
        ringId.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
        ringId.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
        ringId.add("c25ddd596aa7c81fa12378fa725f706d54325d12");

        try {
            FileInputStream inputStream = getContext().openFileInput("Did I exist");
            ////Log.d(TAG,"existed, will query for backup");
            //onCreate = true;
            while (true)
            {
                if(!onCreate)
                    break;
            }
            onCreate=true;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "M", myPort);
            while (true)
            {
                if(!onCreate)
                    break;
            }
        }
        catch (IOException i){
            ////Log.d(TAG,"first time");
            try {
                FileOutputStream outputStream = getContext().openFileOutput("Did I exist", Context.MODE_PRIVATE);
                outputStream.close();
            } catch (Exception e) {
                Log.e("Exception", "File write failed");
            }
        }

        onCreate=false;
        return false;
    }

    private Cursor backup(String key) {
        //onCreate=true;
        MatrixCursor backupCursor = new MatrixCursor(new String[]{"key", "value"});
        ////Log.d(TAG, "Key for backup: "+key);
        for(String s : database.keySet())
        {
            ////Log.d(TAG,"checking for key: "+key+" from: "+s+" substring: "+s.substring(0,3)+" is equal? "+s.substring(0,3).equals(key));
            if(s.substring(0,4).equals(key)) {
                ////Log.d(TAG, "Adding: "+s+" to backupCursor");
                backupCursor.addRow(new String[]{s, database.get(s)});
            }
        }
        backupCursor.moveToFirst();
        backupCursor.close();
        //onCreate=false;
        return backupCursor;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        ////Log.d(TAG, "recieved query: "+selection);
        /*while(true)
        {
            if(!onCreate)
                break;
        }*/
        ////Log.d(TAG, "query allowed: "+selection);
        /*while(true) {
            if (!onCreate) {
                //query++;
                break;
            }
        }*/
        int queryCount = staticQueryCount++;
        Map<String, String> removeMap = new HashMap<String, String>();
        //Log.d(TAG,"269+queryCount: "+queryCount);
        // TODO Auto-generated method stub
        //Log.d(TAG, "Putting false for QueryCount: "+queryCount);
        ready.put(queryCount, false);
        String key = selection;
        //Log.d(TAG, "Putting queryCount: "+queryCount+" = false"+" for query: "+selection);
        String intendedPort = "";
        //Log.d(TAG, "274 Being queried: "+selection);
        boolean lekeChupBaithJa = false;
        if (key.contains(".")) {
            //Log.d(TAG, "277+LekeChupBaithJa true for: "+selection);
            lekeChupBaithJa = true;
            key = key.replace(".", "");
        }
        ////Log.d(TAG, "Putting false in ready for: "+key);
        if (key.equals("*")) {
            ArrayList<String> portsToQueryStar = new ArrayList<String>();
            portsToQueryStar.addAll(ringPort);
            portsToQueryStar.remove(myPortString);
            for (String s : database.keySet())
                resultMap.put("*" + s, database.get(s).substring(1));
            querying('Q', "@.", portsToQueryStar, queryCount);
        } else if (key.equals("@")) {
            if (lekeChupBaithJa) {
                for (String s : database.keySet())
                    resultMap.put("@" + s, database.get(s));
            } else {
                for (String s : database.keySet())
                    resultMap.put("@" + s, database.get(s).substring(1));
            }
            //Log.d(TAG, "330 Putting true in ready for: "+queryCount);
            //Log.d(TAG, "Putting queryCount: "+queryCount+" = true"+" for key: "+selection);
            ready.put(queryCount, true);
        } else {
            String hashKey = "";
            try {
                hashKey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if (lekeChupBaithJa) {
                //Log.d(TAG, "308 lekeChupBaithJa Putting: "+database.get(key)+" for: "+key);
                resultMap.put(key, database.get(key));
                removeMap.put(key,"");
                //Log.d(TAG, "344 Putting true in ready for: "+queryCount+" : "+key);
                //Log.d(TAG, "Putting queryCount: "+queryCount+" = true");
                ready.put(queryCount, true);
            } else {
                intendedPort = getIntendedPort(hashKey);
                ArrayList<String> portsToQuerySingle = new ArrayList<String>();
                portsToQuerySingle.add(intendedPort);
                portsToQuerySingle.add(ringPort.get((ringPort.indexOf(intendedPort) + 1) % 5));
                portsToQuerySingle.add(ringPort.get((ringPort.indexOf(intendedPort) + 2) % 5));
                //Log.d(TAG, "318 Intended port for query: "+key+" = "+intendedPort);
                if (portsToQuerySingle.contains(myPortString)) {
                    portsToQuerySingle.remove(myPortString);
                    //Log.d(TAG, "321 putting: "+database.get(key)+" as: "+intendedPort+key+" for: "+key);
                    resultMap.put(intendedPort + key, database.get(key));
                    removeMap.put(intendedPort+key,"");
                    //Log.d(TAG, "323 I am intended port, fetching: "+key+" from: "+ringPort.get((ringPort.indexOf(intendedPort) + 1) % 5)+" and: "+ringPort.get((ringPort.indexOf(intendedPort) + 2) % 5));

                    ////Log.d(TAG, "Putting true in ready for: "+key);
                }
                //Log.d(TAG,"Querying "+"." + intendedPort + key+" from: "+portsToQuerySingle+" for: "+queryCount+" : "+key);
                querying('Q', "." + intendedPort + key, portsToQuerySingle, queryCount);
            }
        }

        while (true)
        {
            try {
                if (ready.get(queryCount)) {
                    //Log.d(TAG, "Done, removing queryCount: " + queryCount + " for query: " + selection);
                    //ready.remove(queryCount);
                    break;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                //Log.d(TAG, "Did not find: "+queryCount+" in ready");
            }
        }
        /*boolean check;
        while (true) {
            try {
                check = false;
                ////Log.d(TAG, "Query Count: " + queryCount);
                if (ready.get(queryCount) != null) {
                    ////Log.d(TAG, "is ready? " + ready.get(queryCount));
                    check = ready.get(queryCount);
                    ////Log.d(TAG, "check? " + check);
                }
                //check = ready.get(key.contains("@")||key.contains("*")?"@":key)!=null? ready.get(key.contains("@")||key.contains("*")?"@":key):false;
                if (check) {
                    //Log.d(TAG, "breaking");
                    ready.remove(queryCount);
                    check = false;
                    break;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }*/
                ////Log.d(TAG, "Checking readiness of: "+keyToCheck+" for: "+key);
                ////Log.d(TAG, "readiness of: "+keyToCheck+" = "+check);
                //ready.put(queryCount, false);
                mCursorMap.put(key, new MatrixCursor(new String[]{"key", "value"}));
                if (key.equals("@")) {
                    for (String s : resultMap.keySet()) {
                        if (s.contains("@")) {
                            //Log.d(TAG, "358 Adding to mCursorMap: "+resultMap.get(s)+" for: "+s+" as: "+s.substring(5));
                            mCursorMap.get(key).addRow(new String[]{s.substring(5), resultMap.get(s)});
                            removeMap.put(s, "");
                        }
                    }

                } else if (key.equals("*")) {
                    for (String s : resultMap.keySet()) {
                        if (s.contains("*")) {
                            //Log.d(TAG, "367 Adding to mCursorMap: "+resultMap.get(s)+" for: "+key+" as: "+s.substring(5));
                            mCursorMap.get(key).addRow(new String[]{s.substring(5), resultMap.get(s)});
                            removeMap.put(s, "");
                        } else if (s.contains("@")) {
                            //Log.d(TAG, "371 Adding to mCursorMap: "+resultMap.get(s)+" for: "+key+" as: "+s.substring(1));
                            mCursorMap.get(key).addRow(new String[]{s.substring(1), resultMap.get(s)});
                            removeMap.put(s, "");
                        }

                    }
                } else {

                    if (lekeChupBaithJa) {
                        //Log.d(TAG, "380 Adding to mCursorMap: " + key + " for: " + resultMap.get(key));
                        mCursorMap.get(key).addRow(new String[]{key, resultMap.get(key)});
                        removeMap.put(key, "");
                    } else {
                        //Log.d(TAG, "384 Adding to mCursorMap: " + resultMap.get(intendedPort+key) + " for: " + key);
                        ////Log.d(TAG, "Adding to mCursorMap: " + key + " for: " + resultMap.get(key));
                        //for(String s: resultMap.keySet())
                        ////Log.d(TAG, "resultMap contains: " + s + " for: " + resultMap.get(s));
                        mCursorMap.get(key).addRow(new String[]{key, resultMap.get(intendedPort + key)});
                        removeMap.put(intendedPort+key, "");
                    }

                }
                for (String s : removeMap.keySet()) {
                    ////Log.d(TAG, "394 removing from resultMap: "+s);
                    resultMap.remove(s);
                }
                //ready.put(queryCount, false);
        mCursorMap.get(key).moveToFirst();
        mCursorMap.get(key).close();
        MatrixCursor resultCursor = mCursorMap.get(key);
        //Log.d(TAG, "returning: "+mCursorMap.get(key).getString(1)+" with key: "+mCursorMap.get(key).getString(0)+" for: "+selection);
        mCursorMap.remove(key);
        return resultCursor;
    }

    private void querying(char operation, String msg, ArrayList<String> sendToArray, int queryNo) {

        String sendTo = null;
        //Log.d(TAG, "Querying: "+msg+" from: ");
        for (Object s : sendToArray) {
            //Log.d(TAG, ""+s);
            if (sendTo != null)
                sendTo = sendTo + "," + s;
            else
                sendTo = "" + s;
        }
        //Log.d(TAG, "416 Querying: "+operation + ":" + msg + ":" + sendTo + ":" + queryNo);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, operation + ":" + msg + ":" + sendTo + ":" + queryNo, myPort);
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

    private String getIntendedPort(String hashKey) {
        String intendedPort = "";
        for (int i = 0; i < ringId.size(); i++) {
            if (ringId.get(i).compareTo(hashKey) >= 0) {
                intendedPort = ringPort.get(i);
                break;
            }
        }

        if (intendedPort.equals(""))
            intendedPort = ringPort.get(0);

        return intendedPort;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket inputSocket = serverSocket.accept();
                    DataInputStream input = new DataInputStream(inputSocket.getInputStream());
                    String msgIn = input.readUTF();


                    char operation = msgIn.charAt(0);
                    //if(operation == 'M')
                        //onCreate = true;

                    /*while(true)
                    {
                        if(!onCreate)
                            break;
                        else
                        {
                            if(operation == 'M')
                                break;
                        }
                    }*/

                    DataOutputStream output = new DataOutputStream(inputSocket.getOutputStream());
                    if (operation == 'D' || operation == 'I') {
                        output.writeUTF(msgIn);
                        output.close();
                    }


                    switch (operation) {
                        case 'M': {
                            ////Log.d(TAG, "Recieved backup request: "+msgIn);
                            //onCreate=true;
                            String keys = msgIn.split(":")[1];
                            String[] key = keys.split("-");
                            String results = "";
                            for(int i= 0; i<key.length;i++) {
                                Cursor resultCursor = backup(key[i]);
                                if (resultCursor.getCount() > 0) {
                                    resultCursor.moveToFirst();
                                    while (true) {
                                        results = results + ";" + resultCursor.getString(0) + "," + resultCursor.getString(1);
                                        ////Log.d(TAG, "Sending result: "+resultCursor.getString(1)+" for: "+resultCursor.getString(0));
                                        if (resultCursor.isLast())
                                            break;
                                        resultCursor.moveToNext();
                                    }
                                    resultCursor.close();
                                }
                            }

                            //DataOutputStream output1 = new DataOutputStream(inputSocket.getOutputStream());
                            ////Log.d(TAG, "Replying to backup request: "+msgIn);
                            ////Log.d(TAG, "Reply: "+results);
                            output.writeUTF(msgIn + ":" + results);
                            output.flush();
                            //output.close();
                            //onCreate=false;
                            break;
                        }

                        case 'Q': {
                            String key = msgIn.split(":")[1];
                            //Log.d(TAG, "519 Recieved query: "+key);
                            String results = "";
                                Cursor resultCursor = query(mUri, null, key, null, null);
                                if (resultCursor.getCount() > 0) {
                                    resultCursor.moveToFirst();
                                    while (true) {
                                        results = results + ";" + resultCursor.getString(0) + "," + resultCursor.getString(1);
                                        //Log.d(TAG, "527 Sending result: "+resultCursor.getString(1)+" for: "+resultCursor.getString(0));
                                        if (resultCursor.isLast())
                                            break;
                                        resultCursor.moveToNext();
                                    }
                                    resultCursor.close();
                                }

                            //DataOutputStream output2 = new DataOutputStream(inputSocket.getOutputStream());
                            output.writeUTF(msgIn + ":" + results);
                            output.close();
                            break;
                        }
                        case 'D': {
                            String key = msgIn.split(":")[1];
                            delete(mUri, key, null);
                            break;
                        }
                        case 'I': {
                            String keyValue = msgIn.split(":")[1];
                            String key = keyValue.split(",")[0];
                            String value = keyValue.split(",")[1];
                            ContentValues values = new ContentValues();
                            values.put("key", key);
                            values.put("value", value);
                            insert(mUri, values);
                            break;
                        }

                        default:
                            //Log.d(TAG, "Dunno why, but I'm in default");

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            char operation = msgs[0].charAt(0);
            switch (operation) {
                case 'F': {
                    forwarding(msgs[0].substring(2));
                    break;
                }
                case 'Q': {
                    queryResults(msgs[0]);
                    break;
                }
                case 'M': {
                    backup();
                    break;
                }
                default:
                    //Log.d(TAG, "Dunno why, but I'm in default");
            }

            return null;
        }

        private void backup() {
            onCreate=true;
            ArrayList<String> portsToQuery = new ArrayList<String>();
            ArrayList<String> msgToSend = new ArrayList<String>();

            portsToQuery.add(ringPort.get((ringPort.indexOf(myPortString) - 2)<0?(5+(ringPort.indexOf(myPortString) - 2)):(ringPort.indexOf(myPortString) - 2)));
            msgToSend.add("M:" + ringPort.get((ringPort.indexOf(myPortString) - 2)<0?(5+(ringPort.indexOf(myPortString) - 2)):(ringPort.indexOf(myPortString) - 2)));
            portsToQuery.add(ringPort.get((ringPort.indexOf(myPortString) - 1)<0?(5+(ringPort.indexOf(myPortString) - 1)):(ringPort.indexOf(myPortString) - 1)));
            msgToSend.add("M:" + ringPort.get((ringPort.indexOf(myPortString) - 2)<0?(5+(ringPort.indexOf(myPortString) - 2)):(ringPort.indexOf(myPortString) - 2))+"-" + ringPort.get((ringPort.indexOf(myPortString) - 1)<0?(5+(ringPort.indexOf(myPortString) - 1)):(ringPort.indexOf(myPortString) - 1)));
            portsToQuery.add(ringPort.get((ringPort.indexOf(myPortString) + 1) % 5));
            msgToSend.add("M:" + myPortString+"-"+ringPort.get((ringPort.indexOf(myPortString) - 1)<0?(5+(ringPort.indexOf(myPortString) - 1)):(ringPort.indexOf(myPortString) - 1)));
            portsToQuery.add(ringPort.get((ringPort.indexOf(myPortString) + 2) % 5));
            msgToSend.add("M:" + myPortString);

            for (int i = 0; i < portsToQuery.size(); i++) {
                try {
                    ////Log.d(TAG, "Querying: "+msgToSend+" from: "+port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(portsToQuery.get(i)) * 2);

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                    OutputStream msgOut = socket.getOutputStream();
                    DataOutputStream send = new DataOutputStream(msgOut);

                    InputStream ack = socket.getInputStream();
                    DataInputStream msgIn = new DataInputStream(ack);
                    ////Log.d(TAG, "Sending: "+msgToSend.get(i)+" to: "+portsToQuery.get(i));
                    send.writeUTF(msgToSend.get(i));
                    while (true) {
                        String results = msgIn.readUTF();

                        ////Log.d(TAG, "Msg sent was: "+msgToSend.get(i)+" to: "+portsToQuery.get(i)+" recieved: "+(results));
                        if ((results.split(":")[0] + ":" + results.split(":")[1]).equals(msgToSend.get(i))) {
                            if (results.contains(";")) {
                                String values = results.split(":")[2];
                                ////Log.d(TAG, "values: "+values);
                                String[] valueArray = values.split(";");
                                for (int j = 1; j < valueArray.length; j++) {
                                    ////Log.d(TAG, "valueArray["+j+"]: "+valueArray[j]);
                                    String x;
                                    if ((x = database.get(valueArray[j].split(",")[0])) != null) {
                                        ////Log.d(TAG, "x: "+x);
                                        ////Log.d(TAG, "x version: "+Integer.parseInt(x.substring(0, 1)));
                                        ////Log.d(TAG, "recieved version: "+Integer.parseInt(valueArray[j].split(",")[1].substring(0, 1)));
                                        if (Integer.parseInt(x.substring(0, 1)) < Integer.parseInt(valueArray[j].split(",")[1].substring(0, 1))) {
                                            ////Log.d(TAG, "Overwriting: "+valueArray[i].split(",")[1]+" for: "+append + valueArray[i].split(",")[0]);
                                            ////Log.d(TAG, "Overwriting: "+valueArray[j].split(",")[1]+" for: "+valueArray[j].split(",")[0]);
                                            database.put(valueArray[j].split(",")[0], valueArray[j].split(",")[1]);
                                        }
                                    } else {
                                        ////Log.d(TAG, "Not Overwriting, just putting: "+valueArray[i].split(",")[1]+" for: "+append + valueArray[i].split(",")[0]);
                                        ////Log.d(TAG, "Not overwriting: "+valueArray[j].split(",")[1]+" for: "+valueArray[j].split(",")[0]);
                                        database.put(valueArray[j].split(",")[0], valueArray[j].split(",")[1]);
                                    }
                                }
                            }
                        }
                        socket.close();
                        break;
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            ////Log.d(TAG, "Done with backup");
            onCreate = false;
        }

        private void queryResults(String msg) {


                /*while(true){
                    if(queries>0) {
                        queries--;
                        break;
                    }
                }*/


                int queryCount = Integer.parseInt(msg.split(":")[3]);
            //Log.d(TAG, "Allowed query: "+queryCount);
            String remotePorts = msg.split(":")[2];
                ArrayList<String> portsToQuery = new ArrayList<String>();

                Map<String, String> modifyMap = new HashMap<String, String>();
                String msgToSend = msg.split(":")[0] + ":" + msg.split(":")[1];
                String append = "";
                if (msg.split(":")[1].contains("@"))
                    append = "@";
            //Log.d(TAG, "687 msgToSend: "+msgToSend+" append: "+append);
                //if(msg.split(":")[1].contains("*"))
                //append="*";


                for (String x : remotePorts.split(","))
                    portsToQuery.add(x);

                for (String port : portsToQuery) {
                    try{
                    //Log.d(TAG, "Querying: "+msgToSend+" from: "+port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port) * 2);

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                    OutputStream msgOut = socket.getOutputStream();
                    DataOutputStream send = new DataOutputStream(msgOut);

                    InputStream ack = socket.getInputStream();
                    DataInputStream msgIn = new DataInputStream(ack);
                    send.writeUTF(msgToSend);
                    while (true) {
                        String results = msgIn.readUTF();
                        //Log.d(TAG, "712 Msg sent was: "+msgToSend+" recieved: "+(results.split(":")[0] + ":" + results.split(":")[1]));
                        if ((results.split(":")[0] + ":" + results.split(":")[1]).equals(msgToSend)) {
                            if (results.contains(";")) {
                                String values = results.split(":")[2];
                                //Log.d(TAG, "716 results recieved: "+values);
                                String[] valueArray = values.split(";");
                                for (int i = 1; i < valueArray.length; i++) {
                                    //Log.d(TAG, "719 VALUEARRAY: "+valueArray[i].split(",")[1]);
                                    if (!valueArray[i].split(",")[1].equals("null")) {
                                        String x;
                                        if ((x = resultMap.get(append + valueArray[i].split(",")[0])) != null) {
                                            if (Integer.parseInt(x.substring(0, 1)) < Integer.parseInt(valueArray[i].split(",")[1].substring(0, 1))) {
                                                //Log.d(TAG, "726 putting in modifyMap: " + " for: " + append + valueArray[i].split(",")[0]);
                                                //modifyMap.put(append + valueArray[i].split(",")[0], resultMap.get(append + valueArray[i].split(",")[0]).substring(1));
                                                if(!(results.charAt(0)=='.'))
                                                    modifyMap.put(append + valueArray[i].split(",")[0], "");
                                                //Log.d(TAG, "727 Overwriting: " + valueArray[i].split(",")[1] + " for: " + append + valueArray[i].split(",")[0]);
                                                resultMap.put(append + valueArray[i].split(",")[0], valueArray[i].split(",")[1]);
                                            }
                                        } else {
                                            //Log.d(TAG, "731 putting in modifyMap: "+" for: " + append + valueArray[i].split(",")[0]);
                                            //modifyMap.put(append + valueArray[i].split(",")[0], valueArray[i].split(",")[1].substring(1));
                                            modifyMap.put(append + valueArray[i].split(",")[0], "");
                                            //Log.d(TAG, "734 Not Overwriting, just putting: " + valueArray[i].split(",")[1] + " for: " + append + valueArray[i].split(",")[0]);
                                            resultMap.put(append + valueArray[i].split(",")[0], valueArray[i].split(",")[1]);
                                        }
                                    }
                                }
                            }
                            socket.close();
                            break;
                        }
                    }
                }catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    }
                    catch (NumberFormatException e){
                        e.printStackTrace();
                        Log.e(TAG, "NumberFormatException");
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
            }
            }

                for (String s : modifyMap.keySet()) {
                    ////Log.d(TAG, "753 Modifying: "+s+" from: "+resultMap.get(s)+" to: "+resultMap.get(s).substring(1));
                    try {
                        resultMap.put(s, resultMap.get(s).substring(1));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        //Log.d(TAG,"Unable to modify for: "+s);
                    }
                    //Log.d(TAG, "755 Modified: "+s+" to: "+resultMap.get(s));
                }
                modifyMap.clear();
                /*if(msg.split(":")[1].equals("@.")) {
                    ready.put("*", true);
                }
                else {
                    ready.put(msg.split(":")[1], true);
                }*/
                //Log.d(TAG, "806 Putting true for QueryCount: "+queryCount);
            //Log.d(TAG, "Putting queryCount: "+queryCount+" = true for: "+msgToSend);
                ready.put(queryCount, true);
                //queries++;

        }

        private void forwarding(String msg) {
            int count = Integer.parseInt(msg.split(":")[3]);
            String remotePorts = msg.split(":")[2];
            ////Log.d(TAG, "Ports to query: "+remotePorts);
            //String[] remotePort=remotePorts.split("-");

            String msgToSend = msg.split(":")[0] + ":" + msg.split(":")[1];
            //for(int i=0; i<remotePort.length;i++) {
            try {
                ////Log.d(TAG, "Forwarding: "+msgToSend+" to: "+remotePort[i]);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePorts) * 2);

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                OutputStream msgOut = socket.getOutputStream();
                DataOutputStream send = new DataOutputStream(msgOut);
                send.writeUTF(msgToSend);

                InputStream ack = socket.getInputStream();
                DataInputStream msgIn = new DataInputStream(ack);
                while (true) {
                    String acknowledgement = msgIn.readUTF();
                    if (acknowledgement.equals(msgToSend)) {
                        socket.close();
                        break;
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
            }
            //}
            //Log.d(TAG, "Putting Count: "+count+" = true"+" for: "+msg.split(":")[2]);
            ready.put(count, true);
        }

    }
}