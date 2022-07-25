package singularupdatequeue.example;

public class SynchronizedAccount {
    private int balance;

    public SynchronizedAccount(int balance) {
        this.balance = balance;
    }

    public synchronized int credit(int amount) {
        balance += amount;
        return balance;
    }

    public synchronized int debit(int amount) {
        if (balance < amount) {
            throw new IllegalArgumentException("Insufficient balance");
        }
        balance -= amount;
        return balance;
    }
}
