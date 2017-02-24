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
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    private static IQFeed_Socket IQF;
    private static int IQF_PORT = 5009;
    private static String DEFAULT_SYMBOL = "GSK";
    private static String SYMBOL = "symbol";
    private static String TICK_ID = "tickId";
    private static String TICK = "tick";
    private Set<String> accumulator = new HashSet<>();
    private static int COUNT = 0;

    private List<String> symbols;
    // (?<code>P),(?<symbol>[A-Z]+),(?<tickId>[0-9]+),(?<tick>[0-9]*),
    private final Pattern WATCH_PATTERN = Pattern.compile("(?<code>P),(?<symbol>[A-Z]+),(?<tickId>[0-9]+),(?<tick>[0-9]*),");

    public static void main(String[] args) throws IOException {
        Main main = new Main();
        main.execute();
    }

    private void execute() throws IOException {
        symbols = loadSymbols();
        System.out.println("Started");
        IQF = new IQFeed_Socket();
        if (IQF.ConnectSocket(IQF_PORT)) {
            System.out.println("Connected to client");
        } else {
            System.out.println("Not connected :( ");
        }
        IQF.CreateBuffers();
        setProtocol();
        System.out.println("Message posted protocol set.");
        Thread reader = new Thread(iqfReader);
        reader.start();
        setUpdateFields();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(schedule, 0, 1, TimeUnit.SECONDS);
//        getAllUpdateMessage();
//        getTick(DEFAULT_SYMBOL);
    }

    private void setProtocol() throws IOException {
        Java_Config config = new Java_Config();
        IQF.brBufferedWriter.write(String.format("S,SET PROTOCOL,%s\n", config.most_recent_protocol));
        IQF.brBufferedWriter.flush();
    }

    private List<String> loadSymbols() {
        List<String> codes = new ArrayList<>();
        codes.add(DEFAULT_SYMBOL);
        return codes;
    }


    private void setUpdateFields() {
        String command = "S,SELECT UPDATE FIELDS,Symbol,TickID,Tick\r\n";
        writeCommand(command, "Error while selecting update fields");
    }

    private void getAllUpdateMessage() {
        String allUpdateFN = "S,REQUEST ALL UPDATE FIELDNAMES\r\n";
        writeCommand(allUpdateFN, "Error while getting all update filed names");
    }

    private void writeCommand(String command, String errorMsg) {
        try {
            IQF.brBufferedWriter.write(command);
            IQF.brBufferedWriter.flush();
        } catch (IOException e) {
            System.out.println(errorMsg);
        }
    }

    private void getTick(String symbol){
        String tickCommand = "P," + symbol + ",TickID,Tick\r\n";
        writeCommand(tickCommand, "Error while getting tick and tick-id");
    }



    private Runnable schedule = () -> {
        symbols.forEach(this::watch);
    };

    private void watch(String symbol) {
        String command = "w" + symbol + "\r\n";
        System.out.println("watch " + symbol);
        writeCommand(command, "Error while writing to IQFeed");
    }


    private Runnable iqfReader = () -> {
        String line = null;
        try {
            while ((line = IQF.brBufferedReader.readLine()) != null) {
                parseForWatch(line).ifPresent(this::sum);
            }
        } catch (IOException e) {
            System.out.println("Error while reading IQFeed");
        }
    };

    private synchronized void sum(Result res){
        if (!accumulator.add(res.getSymbol())) {
            sendOutput(COUNT);
            COUNT = 0;
            accumulator.clear();
            accumulator.add(res.getSymbol());
        }
        COUNT += res.getTickValue();
    }

    private void sendOutput(int count) {
        System.out.printf("[Time-%d] Count is: %d%n", Calendar.getInstance().getTime().getSeconds(), count);
    }

    Optional<Result> parseForWatch(String line) {
        Matcher matcher = WATCH_PATTERN.matcher(line);
        if (matcher.find()) {
            return Optional.of(new Result(matcher.group(SYMBOL), matcher.group(TICK_ID), matcher.group(TICK)));
        }
        return Optional.empty();
    }
}
