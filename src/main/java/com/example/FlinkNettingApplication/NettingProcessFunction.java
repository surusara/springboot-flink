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
        // Get the current net consideration state, default to 0.0 if not set
        Double currentNetConsideration = netConsiderationState.value();
        if (currentNetConsideration == null) {
            currentNetConsideration = 0.0;
        }

        // Get the current paymentId, create a new one if necessary
        String paymentId = paymentIdState.value();
        String state = stateState.value();

        if (state == null) {
            state = "Open";  // Initial state for a new group
            stateState.update(state);
        } else {
            // Transition to "Intermediate" if the group is already open
            if ("Open".equalsIgnoreCase(state)) {
                state = "Intermediate";
                stateState.update(state);
            }
        }
        // Create a new group if the current state is null or "Closed"
        if (paymentId == null || "Closed".equalsIgnoreCase(state)) {
            paymentId = UUID.randomUUID().toString();
            paymentIdState.update(paymentId);
            currentNetConsideration = 0.0; // Reset the net consideration
            state = "Open"; // New group starts in the "Open" state
            stateState.update(state);
        }

        // Parse the settlement date from the trade
        String settlementDateString = trade.settlementDate;
        LocalDate settlementDate = LocalDate.parse(settlementDateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        long settlementTimestamp = settlementDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

        // Calculate the default window end time
        long defaultWindowEndTime = settlementTimestamp - (3 * 24 * 60 * 60 * 1000L);

        // Determine the window end time based on current time
        long currentTimeMillis = System.currentTimeMillis();
        long windowEndTime = (defaultWindowEndTime > currentTimeMillis)
                ? defaultWindowEndTime + (2 * 60 * 1000L) // Future settlement date
                : currentTimeMillis + (2 * 60 * 1000L);   // Past or today

        // Register the timer for the current window if necessary
        if (windowEndState.value() == null || "Closed".equalsIgnoreCase(state)) {
            windowEndState.update(windowEndTime);
            ctx.timerService().registerProcessingTimeTimer(windowEndTime);
            System.out.println("Window end time registered: " + new Date(windowEndTime));
        }

        // Update net consideration based on the trade operation
        if ("Insert".equalsIgnoreCase(trade.operation)) {
            currentNetConsideration += trade.consideration;
        } else if ("Cancel".equalsIgnoreCase(trade.operation)) {
            currentNetConsideration -= trade.consideration;
        }



        // Update the net consideration state
        netConsiderationState.update(currentNetConsideration);

        // Emit an intermediate result
        NettingResult intermediateResult = new NettingResult();
        intermediateResult.client = trade.client;
        intermediateResult.currency = trade.currency;
        intermediateResult.buySellDirection = trade.buySellDirection;
        intermediateResult.settlementDate = settlementDateString;
        intermediateResult.netConsideration = currentNetConsideration;
        intermediateResult.paymentId = paymentId;
        intermediateResult.state = state;

        out.collect(intermediateResult);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<NettingResult> out) throws Exception {
        // When the window end time is reached, emit the final result
        Double finalNetConsideration = netConsiderationState.value();
        String paymentId = paymentIdState.value();

        // Get the current key for accessing the client and other details
        CompositeKey currentKey = ctx.getCurrentKey();

        // Create and emit the final result
        NettingResult finalResult = new NettingResult();
        finalResult.client = currentKey.client;
        finalResult.currency = currentKey.currency;
        finalResult.buySellDirection = currentKey.buySellDirection;
        finalResult.settlementDate = currentKey.settlementDate;
        finalResult.netConsideration = finalNetConsideration;
        finalResult.paymentId = paymentId;
        finalResult.state = "Closed";

        // Log the timer trigger
        System.out.println("Final timer triggered at: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        out.collect(finalResult);

        // Update the state to "Closed" and clear other states
        stateState.update("Closed");
        netConsiderationState.clear();
        paymentIdState.clear();
        windowEndState.clear();
    }
}
