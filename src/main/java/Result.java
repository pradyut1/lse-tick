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

public class Result {

    private String symbol;
    private String tickId;
    private String tick;
    private final boolean isNull;

    public Result(String symbol, String tickId, String tick) {
        this.symbol = symbol;
        this.tickId = tickId;
        this.tick = tick;
        this.isNull = false;
    }

    public Result(){
       this.isNull = true;
    }

    public boolean isNull() {
        return this.isNull;
    }

    public String getSymbol() {
        if (isNull) {
            throw new RuntimeException("Try to get symbol from null object");
        }
        return symbol;
    }

    public String getTickId() {
        if (isNull) {
            throw new RuntimeException("Try to get tickId from null object");
        }
        return tickId;
    }

    public String getTick() {
        if (isNull) {
            throw new RuntimeException("Try to get tick from null object");
        }
        return tick;
    }

    int getTickValue(){
        int tickValue;
        if (tick.isEmpty()) {
           tickValue = 183;
        } else {
            tickValue = Integer.valueOf(tick);
        }
        if (tickValue == 173) {
            return 1;
        } else if (tickValue == 175) {
            return -1;
        } else if (tickValue == 183) {
            return 0;
        } else {
            throw new RuntimeException("Invalid tick value " + tickValue);
        }
    }
}
