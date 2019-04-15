/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.chapter5.util;

/**
 * POJO representing an alert.
 */
public class Alert {

    public String message;
    public long timestamp;

    /**
     * Empty default constructor to comply with Flink's POJO requirements.
     */
    public Alert() { }

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "(" + message + ", " + timestamp + ")";
    }
}
