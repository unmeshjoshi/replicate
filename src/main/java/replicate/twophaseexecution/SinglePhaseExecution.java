package replicate.twophaseexecution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replicate.common.*;
import replicate.net.InetAddressAndPort;
import replicate.twophaseexecution.messages.*;
import replicate.vsr.CompletionCallback;
import replicate.wal.Command;
import replicate.wal.DurableKVStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * SinglePhaseExecution demonstrates the problematic single-phase execution pattern
 * that leads to inconsistencies and motivates the need for consensus algorithms.
 * 
 * This implementation follows the "execute-first, propagate-later" anti-pattern:
 * 1. Receives client request
 * 2. Executes command immediately on local node
 * 3. Responds "Success" to client immediately
 * 4. Attempts to propagate to other nodes asynchronously
 * 5. If propagation fails or node crashes, system becomes inconsistent
 * 
 * This is exactly the problematic scenario shown in single_phase_execution.puml
 */
public class SinglePhaseExecution extends Replica {
    private static Logger logger = LogManager.getLogger(SinglePhaseExecution.class);
    
    private final DurableKVStore kvStore;
    
    public SinglePhaseExecution(String name, Config config, SystemClock clock, 
                               InetAddressAndPort clientConnectionAddress, 
                               InetAddressAndPort peerConnectionAddress, 
                               List<InetAddressAndPort> peerAddresses) throws IOException {
        super(name, config, clock, clientConnectionAddress, peerConnectionAddress, peerAddresses);
        this.kvStore = new DurableKVStore(config);
    }

    @Override
    protected void registerHandlers() {
        handlesMessage(MessageId.ProposeRequest, this::handlePropagate, ProposeRequest.class);
        handlesRequestAsync(MessageId.ExcuteCommandRequest, this::handleExecute, ExecuteCommandRequest.class);
    }

    /**
     * PROBLEMATIC: Single-phase execution pattern
     * 1. Execute immediately on local node
     * 2. Respond success to client immediately  
     * 3. Try to propagate asynchronously (fire-and-forget)
     */
    CompletableFuture<ExecuteCommandResponse> handleExecute(ExecuteCommandRequest request) {
        byte[] commandBytes = request.command;
        Command command = Command.deserialize(new ByteArrayInputStream(commandBytes));
        
                 logger.info("{} received command: {}", getName(), command.getClass().getSimpleName());
        
        // STEP 1: Execute immediately on local node (PROBLEMATIC!)
        boolean executed = executeCommand(command);
        
        // STEP 2: Respond "Success" to client immediately (PROBLEMATIC!)
        ExecuteCommandResponse response = new ExecuteCommandResponse(Optional.empty(), executed);
        
                 // STEP 3: Try to propagate asynchronously (fire-and-forget)
         if (executed) {
             logger.info("{} executed command locally, now attempting propagation...", getName());
             propagateAsynchronously(commandBytes);
         }
        
        // Return immediate success - this is the problem!
        return CompletableFuture.completedFuture(response);
    }
    
    /**
     * Execute command locally without waiting for consensus
     */
    private boolean executeCommand(Command command) {
        if (command instanceof CompareAndSwap) {
            CompareAndSwap cas = (CompareAndSwap) command;
            Optional<String> existingValue = Optional.ofNullable(kvStore.get(cas.getKey()));
            
                         if (existingValue.equals(cas.getExistingValue())) {
                 kvStore.put(cas.getKey(), cas.getNewValue());
                 logger.info("{} executed: {} = {} (was {})", getName(), cas.getKey(), cas.getNewValue(), existingValue.orElse("null"));
                 return true;
             } else {
                 logger.info("{} execution failed: expected {} but found {}", getName(), cas.getExistingValue().orElse("null"), existingValue.orElse("null"));
                 return false;
             }
        }
        
        throw new IllegalArgumentException("Unknown command: " + command);
    }
    
    /**
     * Attempt to propagate to other nodes asynchronously (fire-and-forget)
     * This is where the problems occur - if this fails, nodes become inconsistent
     */
    private void propagateAsynchronously(byte[] commandBytes) {
        ProposeRequest propagateRequest = new ProposeRequest(commandBytes);
        
        // Use the sendOnewayMessageToOtherReplicas method from parent class
        try {
            logger.info("{} attempting to propagate to other replicas", getName());
            sendOnewayMessageToOtherReplicas(propagateRequest);
        } catch (Exception e) {
            logger.warn("{} failed to propagate: {}", getName(), e.getMessage());
            // PROBLEM: Propagation failure is silently ignored!
            // The local node has already executed and responded success to client
        }
    }
    
    /**
     * Handle propagation from other nodes
     */
    private void handlePropagate(Message<ProposeRequest> message) {
        ProposeRequest request = message.messagePayload();
        byte[] commandBytes = request.getCommand();
        Command command = Command.deserialize(new ByteArrayInputStream(commandBytes));
        
        logger.info("{} received propagation: {}", getName(), command.getClass().getSimpleName());
        
        // Execute the propagated command
        executeCommand(command);
    }
    
    public String getValue(String key) {
        return kvStore.get(key);
    }
    
    /**
     * Simulate node crash by closing the kvStore
     */
    public void close() {
        logger.info("{} is crashing!", getName());
        kvStore.close();
        shutdown();
    }
} 