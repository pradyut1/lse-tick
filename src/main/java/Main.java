/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Main {
    private static IQFeed_Socket IQF;
    private static int IQF_PORT = 5009;
    private static String SYMBOL = "GSK";

    public static void main(String[] args) throws IOException {
        List<String> symbols = new ArrayList<>();
        symbols.add(SYMBOL);
        System.out.println("Started");
        IQF = new IQFeed_Socket();
        if (IQF.ConnectSocket(IQF_PORT)) {
            System.out.println("Connected to client");
        } else {
            System.out.println("Not connected :( ");
        }
        IQF.CreateBuffers();
        Java_Config config = new Java_Config();
        IQF.brBufferedWriter.write(String.format("S,SET PROTOCOL,%s\n", config.most_recent_protocol));
        IQF.brBufferedWriter.flush();
        System.out.println("Message posted protocol set.");
        Thread reader = new Thread(iqfReader);
        reader.start();
        setUpdateFields();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(schedule, 0, 1, TimeUnit.SECONDS);
//        getAllUpdateMessage();
//        getTick(SYMBOL);

    }

    private static void setUpdateFields() {
        String command = "S,SELECT UPDATE FIELDS,Symbol,TickID,Tick\r\n";
        writeCommand(command, "Error while selecting update fields");
    }

    private static void getAllUpdateMessage() {
        String allUpdateFN = "S,REQUEST ALL UPDATE FIELDNAMES\r\n";
        writeCommand(allUpdateFN, "Error while getting all update filed names");
    }

    private static void writeCommand(String command, String errorMsg) {
        try {
            IQF.brBufferedWriter.write(command);
            IQF.brBufferedWriter.flush();
        } catch (IOException e) {
            System.out.println(errorMsg);
        }
    }

    private static void getTick(String symbol){
        String tickCommand = "P," + symbol + ",TickID,Tick\r\n";
        System.out.println("Tick tick");
        writeCommand(tickCommand, "Error while getting tick and tick-id");
    }



    private static Runnable schedule = () -> {
//        String command = "w" + SYMBOL + "\r\n";
        System.out.println("Schedule called");
//        writeCommand(command, "Error while writing to IQFeed");
        getTick(SYMBOL);
    };


    private static Runnable iqfReader = () -> {
        String line = null;
        try {
            while ((line = IQF.brBufferedReader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            System.out.println("Error while reading IQFeed");
        }
    };
}
