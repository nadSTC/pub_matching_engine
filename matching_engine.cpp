/*
    Author : Nad
    Date : 2023-01-15
    Description : A simple C++ Matching Engine
*/

#include <iostream>
#include <sstream>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>

const int ACCOUNT_TABLE_WIDTH = 16;
const int ORDER_TABLE_WIDTH = 24;

struct Transaction
{
    int id;
    int quantity;
    double price;
    int timestamp;
    std::string buyer;
    std::string seller;
    std::string aggressor;
};

struct Account_Details
{
    double usd_balance;
    int coin_balance;
};

struct Order
{
    int id;
    std::string account_name;
    bool is_buy;
    int quantity;
    double price;
    int timestamp;
};

struct OrderComparator
{
    bool operator()(const Order &a, const Order &b) const
    {
        return a.is_buy ? a.price > b.price || (a.price == b.price && a.timestamp < b.timestamp) : a.price < b.price || (a.price == b.price && a.timestamp < b.timestamp);
    }
};

using MessageQueueData = std::variant<Order, std::string>;

std::string convert_dbl_to_string(const double input)
{
    std::ostringstream strs;
    strs << input;
    return strs.str();
}

std::string convert_int_to_string(const int input)
{
    std::ostringstream strs;
    strs << input;
    return strs.str();
}

int convert_string_to_int(std::string &input)
{
    if (input.find_first_not_of("0123456789") != std::string::npos)
    {
        std::cerr << "Invalid input: non-numeric characters found" << std::endl;
        return 0;
    }

    try
    {
        return std::stoi(input);
    }
    catch (const std::invalid_argument &ia)
    {
        return 0;
    }
    catch (const std::out_of_range &oor)
    {
        return 0;
    }
    return 0;
}

double convert_string_to_double(std::string &input)
{
    if (input.find_first_not_of("0123456789.-") != std::string::npos)
    {
        std::cerr << "Invalid input: non-numeric characters found" << std::endl;
        return 0;
    }

    try
    {
        return std::stod(input);
    }
    catch (const std::invalid_argument &ia)
    {
        return 0;
    }
    catch (const std::out_of_range &oor)
    {
        return 0;
    }
    return 0;
}

int get_epoch_ms()
{
    return static_cast<int>(std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count());
}

std::string pretty_print(std::string input, int width)
{
    std::string white_space = "";
    auto extra_spaces = width - input.length();
    if (extra_spaces > 0)
    {
        for (int i = 0; i < extra_spaces / 2; i++)
        {
            white_space += " ";
        }
    }
    return (extra_spaces % 2 != 0 ? " " : "") + white_space + input + white_space;
}

class MessageQueue
{
public:
    void push(const MessageQueueData &data)
    {
        {
            std::lock_guard<std::mutex> lock(message_queue_mutex);
            message_queue.push(data);
        }
        message_available.notify_one();
    }

    MessageQueueData pop()
    {
        std::unique_lock<std::mutex> lock(message_queue_mutex);
        message_available.wait(lock, [this]()
                               { return !message_queue.empty(); });
        MessageQueueData data = message_queue.front();
        message_queue.pop();
        return data;
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock(message_queue_mutex);
        return message_queue.empty();
    }

    void notify() { message_available.notify_one(); }

private:
    std::queue<MessageQueueData> message_queue;
    std::condition_variable message_available;
    mutable std::mutex message_queue_mutex;
};

class OrderBook
{
public:
    void add_order(Order &order)
    {
        std::lock_guard<std::mutex> order_book_lock(order_book_mutex);
        order_book[order.is_buy ? "buy" : "sell"].emplace(order);
    }

    void remove_order(const int &order_id)
    {
        if (order_id == 0)
        {
            return;
        }
        std::lock_guard<std::mutex> lock(order_book_mutex);
        auto &order_book_side = order_book[order_id < 0 ? "buy" : "sell"];
        auto order_it = std::find_if(order_book_side.begin(), order_book_side.end(),
                                     [order_id](const Order &o)
                                     { return o.id == order_id; });
        if (order_it != order_book_side.end())
        {
            order_book_side.erase(order_it);
        }
    }

    void settle_accounts(std::string buyer, std::string seller, int quantity, double price, std::string aggressor, std::map<std::string, Account_Details> &accounts, std::mutex &accounts_mutex, std::vector<Transaction> &transactions, std::mutex &transactions_mutex)
    {
        std::lock_guard<std::mutex> accounts_lock(accounts_mutex);
        std::lock_guard<std::mutex> transactions_lock(transactions_mutex);

        accounts[buyer].coin_balance += quantity;
        accounts[buyer].usd_balance -= quantity * price;
        accounts[seller].coin_balance -= quantity;
        accounts[seller].usd_balance += quantity * price;

        Transaction transaction = {static_cast<int>(transactions.size()) + 1, quantity, price, get_epoch_ms(), buyer, seller, aggressor};
        transactions.push_back(transaction);
        // accounts[buyer].settled_transactions.push_back(buyer_transaction);

        // Transaction seller_transaction = {static_cast<int>(accounts[seller].settled_transactions.size()) + 1, quantity, price, get_epoch_ms(), buyer, seller, aggressor};
        // accounts[seller].settled_transactions.push_back(seller_transaction);
    }

    void match_order(Order &order, std::map<std::string, Account_Details> &accounts, std::mutex &accounts_mutex, std::vector<Transaction> &transactions, std::mutex &transactions_mutex)
    {
        std::lock_guard<std::mutex> order_book_lock(order_book_mutex);
        auto &order_book_side = order_book[order.is_buy ? "sell" : "buy"];
        auto order_it = order_book_side.begin();
        while (order_it != order_book_side.end() && (order.is_buy ? order_it->price <= order.price : order_it->price >= order.price) && order.quantity > 0 && order.account_name != order_it->account_name)
        {
            if (order_it->quantity <= order.quantity)
            {
                order.quantity -= order_it->quantity;
                auto buyer = order.is_buy ? order.account_name : order_it->account_name;
                auto seller = order.is_buy ? order_it->account_name : order.account_name;
                settle_accounts(buyer, seller, order_it->quantity, order_it->price, order.is_buy ? "buy" : "sell", accounts, accounts_mutex, transactions, transactions_mutex);
                order_it = order_book_side.erase(order_it);
            }
            else
            {
                Order updatedOrder;
                updatedOrder.timestamp = order_it->timestamp;
                updatedOrder.id = order_it->id;
                updatedOrder.price = order_it->price;
                updatedOrder.account_name = order_it->account_name;
                updatedOrder.is_buy = order_it->is_buy;
                updatedOrder.quantity = order_it->quantity - order.quantity;

                auto buyer = order.is_buy ? order.account_name : order_it->account_name;
                auto seller = order.is_buy ? order_it->account_name : order.account_name;
                settle_accounts(buyer, seller, order.quantity, order_it->price, order.is_buy ? "buy" : "sell", accounts, accounts_mutex, transactions, transactions_mutex);

                order_book_side.erase(order_it);
                order_book_side.emplace(updatedOrder);

                order.quantity = 0;
                break;
            }
        }
    }

    bool is_allowed_order(Order &order, std::map<std::string, Account_Details> &accounts, std::mutex &accounts_mutex) const
    {
        std::lock_guard<std::mutex> accounts_lock(accounts_mutex);
        std::lock_guard<std::mutex> order_book_lock(order_book_mutex);
        if (order.is_buy)
        {
            return accounts[order.account_name].usd_balance >= order.quantity * order.price && std::find_if(order_book["sell"].begin(), order_book["sell"].end(),
                                                                                                            [order](const Order &o)
                                                                                                            { return o.account_name == order.account_name && o.price == order.price; }) == order_book["sell"].end();
        }
        else
        {
            return accounts[order.account_name].coin_balance >= order.quantity && std::find_if(order_book["buy"].begin(), order_book["buy"].end(),
                                                                                               [order](const Order &o)
                                                                                               { return o.account_name == order.account_name && o.price == order.price; }) == order_book["buy"].end();
        }
    }

    int get_last_order_id(bool isBuy) const
    {
        std::lock_guard<std::mutex> lock(order_book_mutex);
        int last_order_id = 0;
        int factor = isBuy ? 1 : -1;
        auto &order_book_side = order_book[isBuy ? "buy" : "sell"];

        for (auto &order : order_book_side)
        {
            last_order_id = std::max(last_order_id, abs(order.id));
        }

        return factor * (last_order_id == 0 ? 1 : (last_order_id + 1));
    }

    const Order construct_order(std::string account_name, std::string side, int quantity, double price, int timestamp) const
    {
        Order order;
        order.id = this->get_last_order_id(side == "buy");
        order.account_name = account_name;
        order.is_buy = side == "buy";
        order.quantity = quantity;
        order.price = price;
        order.timestamp = timestamp;
        return order;
    }

    void print_order_book() const
    {
        std::cout << "------------------ ORDER BOOK ----------------"
                  << "\n";
        std::lock_guard<std::mutex> lock(order_book_mutex);
        std::cout << " " << pretty_print("Qty", ORDER_TABLE_WIDTH / 2) << " | " << pretty_print("$", ORDER_TABLE_WIDTH / 2)
                  << "\n";

        std::map<double, int> buy_orders_by_price;
        std::map<double, int> sell_orders_by_price;
        for (auto buy_it = order_book["buy"].begin(), sell_it = order_book["sell"].begin();
             buy_it != order_book["buy"].end() || sell_it != order_book["sell"].end();)
        {
            if (buy_it != order_book["buy"].end())
            {
                buy_orders_by_price[buy_it->price] += buy_it->quantity;
                buy_it++;
            }
            if (sell_it != order_book["sell"].end())
            {
                sell_orders_by_price[sell_it->price] += sell_it->quantity;
                sell_it++;
            }
        }

        for (auto sell_it = sell_orders_by_price.rbegin(); sell_it != sell_orders_by_price.rend();)
        {
            std::cout << "⌄" << pretty_print(convert_int_to_string(sell_it->second), ORDER_TABLE_WIDTH / 2) << " | " << pretty_print(convert_dbl_to_string(sell_it->first), ORDER_TABLE_WIDTH / 2) << "\n";
            sell_it++;
        }

        std::cout << "\n";

        for (auto buy_it = buy_orders_by_price.rbegin(); buy_it != buy_orders_by_price.rend();)
        {
            std::cout << "⌃" << pretty_print(convert_int_to_string(buy_it->second), ORDER_TABLE_WIDTH / 2) << " | " << pretty_print(convert_dbl_to_string(buy_it->first), ORDER_TABLE_WIDTH / 2) << "\n";
            buy_it++;
        }
    }

private:
    mutable std::map<std::string, std::multiset<Order, OrderComparator>> order_book;
    mutable std::mutex order_book_mutex;
};

std::map<std::string, Account_Details> accounts;
std::mutex accounts_mutex;
std::vector<Transaction> transactions;
std::mutex transactions_mutex;
OrderBook order_book;

void parse_input(std::string &input, std::vector<std::string> &input_tokens)
{
    std::stringstream ss(input);
    std::string token;

    while (ss >> token)
    {
        input_tokens.push_back(token);
    }
}

void print_accounts()
{
    std::lock_guard<std::mutex> lock(accounts_mutex);
    std::cout << "-------------------- ACCOUNTS ------------------"
              << "\n";
    std::cout << "Total Accounts: " << accounts.size() << "\n";
    std::cout << pretty_print("Account", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("$USD", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("Coin (C)", ACCOUNT_TABLE_WIDTH) << "\n";
    for (auto account : accounts)
    {
        std::cout << pretty_print(account.first, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(convert_dbl_to_string(account.second.usd_balance), ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(convert_int_to_string(account.second.coin_balance), ACCOUNT_TABLE_WIDTH) << "\n";
    }
}

void print_transactions(std::string &account, int num_transactions)
{
    std::lock_guard<std::mutex> lock(accounts_mutex);
    std::cout << "-------------------- TRANSACTIONS ------------------"
              << "\n";
    if (account == "")
    {
        std::cout << "All Transactions"
                  << "\n";
    }
    else
    {
        std::cout << "Account: " << account << "\n";
    }
    std::cout << pretty_print("Timestamp", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("Aggressor", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("Buyer", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("Seller", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("Quantity", ACCOUNT_TABLE_WIDTH) << " | " << pretty_print("Price", ACCOUNT_TABLE_WIDTH) << "\n";
    auto transaction_iter = transactions.rbegin();
    int i = 0;
    while (transaction_iter != transactions.rend() && i < num_transactions)
    {
        if (account != "" && (transaction_iter->buyer == account || transaction_iter->seller == account))
        {
            std::cout << pretty_print(convert_int_to_string(transaction_iter->timestamp), ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(transaction_iter->aggressor, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(transaction_iter->buyer, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(transaction_iter->seller, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(convert_int_to_string(transaction_iter->quantity), ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(convert_dbl_to_string(transaction_iter->price), ACCOUNT_TABLE_WIDTH) << "\n";
            transaction_iter++;
            i++;
        }
        else if (account == "")
        {
            std::cout << pretty_print(convert_int_to_string(transaction_iter->timestamp), ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(transaction_iter->aggressor, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(transaction_iter->buyer, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(transaction_iter->seller, ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(convert_int_to_string(transaction_iter->quantity), ACCOUNT_TABLE_WIDTH) << " | " << pretty_print(convert_dbl_to_string(transaction_iter->price), ACCOUNT_TABLE_WIDTH) << "\n";
            transaction_iter++;
            i++;
        }
        else
        {
            transaction_iter++;
        }
    }
}

void command_router(std::vector<std::string> &input_tokens, MessageQueue &message_queue)
{
    if (input_tokens[0] == "account")
    {
        if (input_tokens.size() < 3)
        {
            std::cout << "Invalid arguments\n";
            return;
        }

        if (input_tokens[2] == "create")
        {
            {
                std::lock_guard<std::mutex> lock(accounts_mutex);
                accounts.emplace(std::make_pair(input_tokens[1], Account_Details{0, 0}));
            }
            print_accounts();
            return;
        }

        if (input_tokens[2] == "delete")
        {
            {
                std::lock_guard<std::mutex> lock(accounts_mutex);
                accounts.erase(input_tokens[1]);
            }
            print_accounts();
            return;
        }

        std::unique_lock<std::mutex> lock(accounts_mutex);
        auto Account_Details = accounts.find(input_tokens[1]);
        if (Account_Details == accounts.end())
        {
            std::cout << "Account does not exist\n";
            return;
        }
        lock.unlock();

        if (input_tokens[2] == "query")
        {
            std::cout << "Account Name: " << input_tokens[1] << "\n";
            std::cout << "USD Balance: " << Account_Details->second.usd_balance << "\n";
            std::cout << "Coin Balance: " << Account_Details->second.coin_balance << "\n";
        }
        else if (input_tokens[2] == "fund")
        {
            if (input_tokens.size() < 4)
            {
                std::cout << "Invalid arguments\n";
                return;
            }
            double funding_amount = convert_string_to_double(input_tokens[3]);
            std::lock_guard<std::mutex> lock(accounts_mutex);
            Account_Details->second.usd_balance += funding_amount;
        }
        else if (input_tokens[2] == "withdraw")
        {
            if (input_tokens.size() < 4)
            {
                std::cout << "Invalid arguments\n";
                return;
            }
            double withdraw_amount = convert_string_to_double(input_tokens[3]);
            std::lock_guard<std::mutex> lock(accounts_mutex);
            if (withdraw_amount > Account_Details->second.usd_balance)
            {
                std::cout << "Insufficient funds\n";
                return;
            }
            Account_Details->second.usd_balance -= withdraw_amount;
            std::cout << "Sending " << withdraw_amount << " to " << input_tokens[1] << "'s linked bank account."
                      << "\n";
        }
        else if (input_tokens[2] == "transactions")
        {
            if (input_tokens.size() < 4)
            {
                std::cout << "Invalid arguments\n";
                return;
            }
            int num_transactions = convert_string_to_int(input_tokens[3]);
            print_transactions(input_tokens[1], num_transactions);
        }
        else
        {
            std::cout << "Invalid subcommand\n";
        }
    }
    else if (input_tokens[0] == "order")
    {
        if (input_tokens.size() < 2)
        {
            std::cout << "Invalid arguments\n";
            return;
        }

        if (input_tokens[1] == "cancel")
        {
            if (input_tokens.size() < 3)
            {
                std::cout << "Invalid arguments\n";
                return;
            }
            int order_id = convert_string_to_int(input_tokens[2]);
            if (order_id == 0)
            {
                return;
            }
            message_queue.push(input_tokens[1] + " " + input_tokens[2]);
            message_queue.notify();
        }
        else if (input_tokens[1] == "create")
        {
            if (input_tokens.size() < 6)
            {
                std::cout << "Invalid arguments\n";
                return;
            }
            std::string account_name = input_tokens[2];
            std::string side = input_tokens[3];
            auto quantity = convert_string_to_int(input_tokens[4]);
            auto price = convert_string_to_double(input_tokens[5]);

            // create order
            auto order = order_book.construct_order(account_name, side, quantity, price, get_epoch_ms());
            message_queue.push(order);
        }
        else
        {
            std::cout << "Invalid subcommand\n";
        }
    }
    else if (input_tokens[0] == "state")
    {
        print_accounts();
        order_book.print_order_book();
    }
    else if (input_tokens[0] == "transactions")
    {
        if (input_tokens.size() < 2)
        {
            std::cout << "Invalid arguments\n";
            return;
        }
        int num_transactions = convert_string_to_int(input_tokens[1]);
        std::string all = "";
        print_transactions(all, num_transactions);
    }
    else
    {
        std::cout << "Invalid command\n";
    }
}

/*
    Matching Engine
*/
void matching_engine(MessageQueue &message_queue)
{
    while (true)
    {
        // Wait for a message to be available
        MessageQueueData data = message_queue.pop();

        if (data.index() == 0) // data is of type Order
        {
            Order order = std::get<Order>(data);
            if (order_book.is_allowed_order(order, accounts, accounts_mutex))
            {
                order_book.match_order(order, accounts, accounts_mutex, transactions, transactions_mutex);
                if (order.quantity > 0)
                {
                    order_book.add_order(order);
                }
            }
            else
            {
                std::cout << "Order rejected\n";
            }
        }
        else if (data.index() == 1) // data is of type std::string
        {
            std::vector<std::string> message_tokens;
            parse_input(std::get<std::string>(data), message_tokens);

            if (message_tokens.size() == 0)
            {
                continue;
            }

            if (message_tokens[0] == "exit")
            {
                break;
            }

            if (message_tokens[0] == "cancel")
            {
                int order_id = convert_string_to_int(message_tokens[1]);
                order_book.remove_order(order_id);
            }
        }
    }
}

/*
    Command Line Interface
*/
void commander(MessageQueue &message_queue)
{
    while (true)
    {

        std::string input;
        std::vector<std::string> input_tokens;
        std::cout << "Enter Command: ";
        if (!getline(std::cin, input))
        {
            break;
        }

        parse_input(input, input_tokens);

        if (input_tokens.size() == 0)
        {
            continue;
        }

        if (input_tokens[0] == "exit")
        {
            break;
        }

        command_router(input_tokens, message_queue);
    }
}

// update as you see fit
void setup()
{
    {
        std::lock_guard<std::mutex> lock(accounts_mutex);
        accounts.emplace("alice", Account_Details{6000, 43540});
        accounts.emplace("bob", Account_Details{300, 2000});
        accounts.emplace("charlie", Account_Details{1235, 1000});
    }
    auto order1 = order_book.construct_order("alice", "buy", 1, 20.50, get_epoch_ms());
    order_book.add_order(order1);
    auto order2 = order_book.construct_order("bob", "buy", 10, 22.50, get_epoch_ms());
    order_book.add_order(order2);
    auto order3 = order_book.construct_order("charlie", "sell", 8, 23.50, get_epoch_ms());
    order_book.add_order(order3);
    auto order4 = order_book.construct_order("charlie", "sell", 8, 25.50, get_epoch_ms());
    order_book.add_order(order4);
}

int main()
{
    setup();
    MessageQueue message_queue;
    std::thread commander_thread(commander, std::ref(message_queue));
    std::thread matching_engine_thread(matching_engine, std::ref(message_queue));

    commander_thread.join();
    matching_engine_thread.join();

    return 0;
}