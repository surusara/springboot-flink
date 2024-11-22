package com.example.FlinkNettingApplication;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.UUID;
import java.time.LocalDateTime;

public class NettingProcessFunction extends KeyedProcessFunction<CompositeKey, Trade, NettingResult> {

    private ValueState<Double> netConsiderationState;
    private ValueState<String> paymentIdState;
    private ValueState<Long> windowEndState;
    private ValueState<String> stateState;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        // State for tracking net consideration
        ValueStateDescriptor<Double> netConsiderationDescriptor = new ValueStateDescriptor<>(
                "netConsiderationState", Double.class);
        netConsiderationState = getRuntimeContext().getState(netConsiderationDescriptor);

        // State for tracking paymentId (created at window closure)
        ValueStateDescriptor<String> paymentIdDescriptor = new ValueStateDescriptor<>(
                "paymentIdState", String.class);
        paymentIdState = getRuntimeContext().getState(paymentIdDescriptor);

        // State for tracking the window's end time
        ValueStateDescriptor<Long> windowEndDescriptor = new ValueStateDescriptor<>(
                "windowEndState", Long.class);
        windowEndState = getRuntimeContext().getState(windowEndDescriptor);

        // Define ValueState for state (Initial, Intermediate, Final)
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>(
                "stateState", String.class);
        stateState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(Trade trade, Context ctx, Collector<NettingResult> out) throws Exception {
        // Get current state value for net consideration, default to 0.0 if not set
        Double currentNetConsideration = netConsiderationState.value();
        if (currentNetConsideration == null) {
            currentNetConsideration = 0.0;
        }

        // Get current paymentId, create one if not set
        String paymentId = paymentIdState.value();
        if (paymentId == null) {
            paymentId = UUID.randomUUID().toString();
            paymentIdState.update(paymentId);
        }

        // Get the settlementDate as String
        String settlementDateString = trade.settlementDate;  // This is a String

        LocalDate settlementDate = LocalDate.parse(settlementDateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        long settlementTimestamp = settlementDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

        // Calculate the default window end time (3 days before the settlement date)
        long defaultWindowEndTime = settlementTimestamp - (3 * 24 * 60 * 60 * 1000L);  // settlementDate - 3 days

        // Current system time (in milliseconds)
        long currentTimeMillis = System.currentTimeMillis();

        long windowEndTime;

        // Decide the window end time based on the settlement date
        if (defaultWindowEndTime > currentTimeMillis) {
            // Scenario 1: Settlement date is in the future, use (settlementDate - 3 days) + 2 minutes
            windowEndTime = defaultWindowEndTime + (2 * 60 * 1000L);  // Add 2 minutes
        } else {
            // Scenario 2: Settlement date is today or earlier, terminate 2 minutes from now
            windowEndTime = currentTimeMillis + (2 * 60 * 1000L);
        }

        // Update the windowEndState and register the timer
        if (windowEndState.value() == null ) {
            windowEndState.update(windowEndTime);
            //ctx.timerService().registerEventTimeTimer(windowEndTime);
            ctx.timerService().registerProcessingTimeTimer(windowEndTime);
            // Log or convert the windowEndTime for clarity
            Date dtwindowEndTime = new Date(windowEndTime);
            System.out.println("Window End Time Set to : " + dtwindowEndTime);
        } else {
            // Log the existing window end time for reference
            Date dtExistingWindowEndTime = new Date(windowEndState.value());
            System.out.println("Existing Window End Time: " + dtExistingWindowEndTime);
        }

        // Set initial state to "Initial" if not set
        String state = stateState.value();
        if (state == null) {
            state = "Initial";  // Initial state for a new group
            stateState.update(state);
        } else {
            // Update the state to "Intermediate" if it's not "Final"
            if (!"Final".equalsIgnoreCase(state)) {
                state = "Intermediate";
                stateState.update(state);
            }
        }

        // Update the net consideration based on the trade operation
        if ("Insert".equalsIgnoreCase(trade.operation)) {
            currentNetConsideration += trade.consideration;
        } else if ("Cancel".equalsIgnoreCase(trade.operation)) {
            currentNetConsideration -= trade.consideration;
        }

        // Update the net consideration state
        netConsiderationState.update(currentNetConsideration);

        // Emit intermediate netting result with state "Intermediate"
        NettingResult intermediateResult = new NettingResult();
        intermediateResult.client = trade.client;
        intermediateResult.currency = trade.currency;
        intermediateResult.buySellDirection = trade.buySellDirection;
        intermediateResult.settlementDate = settlementDateString;  // Keep the original string format
        intermediateResult.netConsideration = currentNetConsideration;
        intermediateResult.paymentId = paymentId;
        intermediateResult.state = state;  // Emit the state (Initial or Intermediate)

        // Emit intermediate result in real-time
        out.collect(intermediateResult);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<NettingResult> out) throws Exception {
        // When the window end time is reached, emit the final netting result
        Double finalNetConsideration = netConsiderationState.value();
        String paymentId = paymentIdState.value();

        // Get the key (CompositeKey) to access its properties (e.g., client, currency, etc.)
        CompositeKey currentKey = ctx.getCurrentKey();

        // Create and emit the final netting result for the group
        NettingResult result = new NettingResult();

        // Ensure these fields exist in your CompositeKey class
        result.client = currentKey.client;  // Accessing client from CompositeKey
        result.currency = currentKey.currency;  // Accessing currency from CompositeKey
        result.buySellDirection = currentKey.buySellDirection;  // Accessing buySellDirection from CompositeKey

        // Directly access settlementDate (since it's a String)
        result.settlementDate = currentKey.settlementDate;  // Accessing settlementDate (String directly)

        result.netConsideration = finalNetConsideration;
        result.paymentId = paymentId;
        result.state = "Final";  // Set the state to "Final" when the window ends
        // Log the current date and time
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Final window timer triggered: " + now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        // Emit the final netting result
        out.collect(result);

        // Clean up all states associated with the current key
        netConsiderationState.clear();
        paymentIdState.clear();
        windowEndState.clear();
        stateState.clear();
    }
}
