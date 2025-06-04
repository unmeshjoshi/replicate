package replicate.chain;

public enum NodeRole {
    HEAD,    // Accepts writes
    MIDDLE,  // Forwards writes
    TAIL,    // Handles reads and commits
    JOINING  // Temporary state during configuration changes
} 