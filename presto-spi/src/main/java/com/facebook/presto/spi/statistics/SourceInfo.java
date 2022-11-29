/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.statistics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Describes source of PlanStatistics.
 * We have different ways to estimate PlanStatistics like Cost based optimizer and History based optimizer.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        property = "sourceInfo")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CostBasedSourceInfo.class),
        @JsonSubTypes.Type(value = HistoryBasedSourceInfo.class)})
public abstract class SourceInfo
{
    public abstract boolean isConfident();
}
