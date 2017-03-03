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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
import java.util.logging.Logger;
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
    private static String symbolsFilePath;
    private static File symbolsFile;
    private static String resultCSVFilePath;
    private static File resultCSVFile;
    private static String workingDir;
    private BufferedWriter bw;

    private List<String> symbols;
    // (?<code>P),(?<symbol>[A-Z]+),(?<tickId>[0-9]+),(?<tick>[0-9]*),
    private final Pattern WATCH_PATTERN = Pattern.compile("(?<code>P),(?<symbol>[A-Z]+),(?<tickId>[0-9]+),(?<tick>[0-9]*),");
    private static final Logger log = Logger.getLogger(Main.class.getName());

    public Main(String[] inputs) throws IOException {
        checkInputs(inputs);
    }

    public static void main(String[] args) throws IOException {
        Main main = new Main(args);
        main.execute();
    }

    private void checkInputs(String[] inputs) throws IOException {
        if (inputs == null || inputs.length != 2) {
            throw new RuntimeException("Invalid inputs, use <jar> <symbols> <csvfile> ");
        }

        workingDir = System.getProperty("user.dir");
//        symbolsFilePath = workingDir + File.separator + inputs[0];
//        symbolsFile = new File(symbolsFilePath);
//        if (!symbolsFile.exists()) {
//            throw new RuntimeException("Symbol file doesn't exist -> " + symbolsFilePath);
//        }

        resultCSVFilePath = workingDir + File.separator + inputs[1];
        resultCSVFile = new File(resultCSVFilePath);
        if (resultCSVFile.createNewFile()) {
            log.info("Result file created");
        }
        bw = new BufferedWriter(new FileWriter(resultCSVFile));
    }

    private void execute() throws IOException {
        symbols = loadSymbols();
        log.info("Started");
        IQF = new IQFeed_Socket();
        if (IQF.ConnectSocket(IQF_PORT)) {
            log.info("Connected to client");
        } else {
            log.info("Not connected :( ");
        }
        IQF.CreateBuffers();
        setProtocol();
        log.info("Message posted protocol set.");
        Thread reader = new Thread(iqfReader);
        reader.start();
        setUpdateFields();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::watchSymbols, 0, 1, TimeUnit.SECONDS);
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
//        try (BufferedReader br = new BufferedReader(new FileReader(symbolsFile))) {
//          String line = null;
//            while ((line = br.readLine()) != null) {
//                codes.add(line.trim().toUpperCase());
//            }
//        } catch (IOException e) {
//            log.severe("Error loading symbols");
//        }
//        log.info("Loaded " + codes.size() + " symbols");
        codes.add("JTLT.Z");
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
            log.severe(errorMsg);
        }

    }

    private void getTick(String symbol){
        String tickCommand = "P," + symbol + ",TickID,Tick\r\n";
        writeCommand(tickCommand, "Error while getting tick and tick-id");
    }


    private void watchSymbols() {
        symbols.forEach(this::watch);
    }

    private void watch(String symbol) {
        String command = "w" + symbol + "\r\n";
        log.finer("watch " + symbol);
        writeCommand(command, "Error while writing to IQFeed");
    }


    private Runnable iqfReader = () -> {
        String line = null;
        try {
            while ((line = IQF.brBufferedReader.readLine()) != null) {
                parseForWatch(line).ifPresent(this::sum);
            }
        } catch (IOException e) {
            log.severe("Error while reading IQFeed");
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

    private void sendOutput(int count){
        String str = buildOutput(getDateAndTime(), count);
        try {
            bw.newLine();
            bw.write(str);
            bw.flush();
            log.info(str);
        } catch (IOException e) {
            log.severe("Error writing  to CSV file, -> " + str);
        }
    }

    Optional<Result> parseForWatch(String line) {
        Matcher matcher = WATCH_PATTERN.matcher(line);
        if (matcher.find()) {
            return Optional.of(new Result(matcher.group(SYMBOL), matcher.group(TICK_ID), matcher.group(TICK)));
        }
        return Optional.empty();
    }

    private String buildOutput(String dateAndTime, int count) {
        return new StringBuilder(dateAndTime)
                .append(",")
                .append(count)
                .toString();
    }

    private String getDateAndTime(){
        Calendar instance = Calendar.getInstance();
        return new StringBuilder(instance.get(Calendar.YEAR))
                .append(instance.get(Calendar.MONTH))
                .append(instance.get(Calendar.DATE))
                .append(",")
                .append(instance.get(Calendar.HOUR))
                .append(instance.get(Calendar.MINUTE))
                .append(instance.get(Calendar.SECOND))
                .toString();
    }

}
