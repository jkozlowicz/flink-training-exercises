/*
 * Copyright 2018 data Artisans GmbH
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

package com.dataartisans.flinktraining.solutions.datastream_scala.broadcast

import java.util.Map.Entry

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase.{printOrTest, rideSourceOrTest}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * The "Nearest Future Taxi" exercise of the Flink training
  * (http://training.ververica.com).
  *
  * Given a location that is broadcast, the goal of this exercise is to watch the stream of
  * taxi rides and report on taxis that complete rides closest to the requested location.
  * The application should be able to handle simultaneous queries.
  *
  * Parameters:
  * -input path-to-input-file
  *
  * Use nc -lk 9999 to establish a socket stream from stdin on port 9999
  *
  * Some good locations:
  *
  * -74, 41 					(Near, but outside the city to the NNW)
  * -73.7781, 40.6413 			(JFK Airport)
  * -73.977664, 40.761484		(Museum of Modern Art)
  */

case class Query(queryId: Long, longitude: Float, latitude: Float)

object Query {
  def apply(longitude: Float, latitude: Float): Query = new Query(Random.nextLong, longitude, latitude)
}

object NearestTaxiSolution {

  val queryDescriptor = new MapStateDescriptor[Long, Query]("queries",
    createTypeInformation[Long], createTypeInformation[Query])

  def main(args: Array[String]): Unit = {
    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val ridesFile = params.get("input", ExerciseBase.pathToRideData)

    val maxEventDelay = 60        // events are out of order by at most 60 seconds
    val servingSpeedFactor = 600  // 10 minutes worth of events are served every second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(rideSourceOrTest(
      new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))

    val splitQuery = (msg: String) => {
      val parts = msg.split(",\\s*").map(_.toFloat)
      Query(parts(0), parts(1))
    }
    // add a socket source
    val queryStream = env.socketTextStream("localhost", 9999)
      .map(splitQuery)
      .broadcast(queryDescriptor)

    val reports = rides
      .keyBy(_.taxiId)
      .connect(queryStream)
      .process(new QueryFunction)

    val nearest = reports
      .keyBy(_._1)
      .process(new ClosestTaxi)

    printOrTest(nearest)

    env.execute("Nearest Available Taxi")
  }

  // Only pass thru values that are new minima -- remove duplicates.
  class ClosestTaxi extends KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)] {
    // store (taxiId, distance), keyed by queryId
    lazy val closetState: ValueState[(Long, Float)] = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Float)]("report", createTypeInformation[(Long, Float)]))

    // in and out tuples: (queryId, taxiId, distance)
    override def processElement(report: (Long, Long, Float),
                                context: KeyedProcessFunction[Long, (Long, Long, Float), (Long, Long, Float)]#Context,
                                out: Collector[(Long, Long, Float)]): Unit =
      if (closetState.value == null || report._3 < closetState.value._2) {
        closetState.update((report._2, report._3))
        out.collect(report)
      }
  }

  // Note that in order to have consistent results after a restore from a checkpoint, the
  // behavior of this method must be deterministic, and NOT depend on characterisitcs of an
  // individual sub-task.
  class QueryFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]{

    override def processElement(ride: TaxiRide,
                                readOnlyContext: KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]#ReadOnlyContext,
                                out: Collector[(Long, Long, Float)]): Unit =
      if (!ride.isStart) for (entry: Entry[Long, Query] <-
             readOnlyContext.getBroadcastState(queryDescriptor).immutableEntries()) {
        val q = entry.getValue
        val dist = ride.getEuclideanDistance(q.longitude, q.latitude).toFloat
        out.collect((entry.getKey, ride.taxiId, dist))
      }

    override def processBroadcastElement(query: Query,
                                         context: KeyedBroadcastProcessFunction[Long, TaxiRide, Query, (Long, Long, Float)]#Context,
                                         out: Collector[(Long, Long, Float)]): Unit = {
      println("New query: " + query)
      context.getBroadcastState(queryDescriptor).put(query.queryId, query)
    }
  }

}