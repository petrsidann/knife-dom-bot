#!/usr/bin/env python3
import os, sys, time, json, logging, threading
from decimal import Decimal, getcontext
import zmq
from dotenv import load_dotenv

load_dotenv()
getcontext().prec = 16

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("knife_dom.log")]
)
logger = logging.getLogger("KNIFE-DOM")

ZMQ_REQ_SERVER = "tcp://127.0.0.1:5555"
ZMQ_SUB_SERVER = "tcp://127.0.0.1:5556"
STATE_FILE = "knife_dom_state.json"
SYMBOL = "SOLUSDT"
INITIAL_CAPITAL = Decimal("50.0")
MAX_DAILY_LOSS = Decimal("0.10")

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        data["capital"] = Decimal(str(data.get("capital", str(INITIAL_CAPITAL))))
        data.setdefault("in_position", False)
        data.setdefault("daily_pnl", 0.0)
        data.setdefault("reset_day", time.localtime().tm_yday)
        return data
    return {
        "capital": INITIAL_CAPITAL,
        "in_position": False,
        "daily_pnl": 0.0,
        "reset_day": time.localtime().tm_yday,
        "trade_history": [],
    }

def save_state(state):
    snapshot = {
        "capital": str(state["capital"]),
        "in_position": state["in_position"],
        "daily_pnl": state["daily_pnl"],
        "reset_day": state["reset_day"],
        "trade_history": state["trade_history"][-100:],
    }
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snapshot, f, indent=2)
    os.replace(tmp, STATE_FILE)

state = load_state()
capital = state["capital"]
in_position = state["in_position"]

context = zmq.Context()
req_socket = context.socket(zmq.REQ)
req_socket.connect(ZMQ_REQ_SERVER)
req_socket.setsockopt(zmq.RCVTIMEO, 2000)

sub_socket = context.socket(zmq.SUB)
sub_socket.connect(ZMQ_SUB_SERVER)
sub_socket.setsockopt(zmq.SUBSCRIBE, b"")
sub_socket.setsockopt(zmq.RCVTIMEO, 100)

def send_signal(action, price=0, size=0, sl=0, tp=0, leverage=5, boost=False,
                reason="", imbalance=0.0, slope=0.0, atr=0.0):
    payload = {
        "action": action,
        "symbol": SYMBOL,
        "price": str(price),
        "size": str(size),
        "sl": str(sl),
        "tp": str(tp),
        "leverage": leverage,
        "boost": boost,
        "reason": reason,
        "imbalance": str(imbalance),
        "slope": str(slope),
        "atr": str(atr),
    }
    try:
        req_socket.send_string(json.dumps(payload))
        resp = req_socket.recv_string()
        return json.loads(resp)
    except Exception as e:
        logger.error(f"ZMQ error: {e}")
        return None

def fill_report_listener():
    global capital, in_position, state
    while True:
        try:
            msg = sub_socket.recv_string()
            report = json.loads(msg)
            symbol = report.get("symbol", "UNKNOWN")
            is_exit = report.get("is_exit", False)
            pnl = Decimal(str(report.get("pnl", 0.0)))
            mode = report.get("mode", "base")
            reason = report.get("signal_reason", "")
            imbalance = Decimal(str(report.get("imbalance", 0.0)))
            slope = Decimal(str(report.get("slope", 0.0)))
            atr = Decimal(str(report.get("atr", 0.0)))
            mfe = Decimal(str(report.get("mfe", 0.0)))
            mae = Decimal(str(report.get("mae", 0.0)))

            if is_exit:
                capital += pnl
                in_position = False
                state["capital"] = capital
                state["in_position"] = in_position
                state["daily_pnl"] += float(pnl)
                save_state(state)
                logger.info(f"📤 Exit: {symbol} PnL=${pnl:.2f} Capital=${capital:.2f} | Mode: {mode} | Reason: {reason} | Imb: {imbalance:.2f} | Slope: {slope:.5f} | ATR: {atr:.4f} | MFE=${mfe:.4f}, MAE=${mae:.4f}")
            else:
                in_position = True
                state["in_position"] = True
                save_state(state)
                logger.info(f"📥 Entry: {symbol} filled | Mode: {mode} | Reason: {reason} | Imb: {imbalance:.2f} | ATR: {atr:.4f}")

        except zmq.Again:
            continue
        except Exception as e:
            logger.error(f"Fill listener error: {e}")
            time.sleep(1)

threading.Thread(target=fill_report_listener, daemon=True).start()

def main():
    global capital, in_position, state
    logger.info("🗡️ KNIFE DOM v8.12.1 — Final Production Build (Testnet)")
    logger.info(f"💎 Starting Capital: ${capital:.2f}")

    while True:
        try:
            today = time.localtime().tm_yday
            if state["reset_day"] != today:
                state["daily_pnl"] = 0.0
                state["reset_day"] = today
                save_state(state)

            if state["daily_pnl"] < -float(capital) * float(MAX_DAILY_LOSS):
                logger.warning("Daily loss limit reached. Pausing for 1 hour.")
                time.sleep(3600)
                continue

            cmd = sys.stdin.readline().strip().lower()
            if cmd == "close":
                if in_position:
                    resp = send_signal("Close")
                    logger.info(f"Close command sent: {resp}")
                else:
                    logger.info("No position to close")
            elif cmd == "status":
                logger.info(f"Capital: ${capital:.2f} | In position: {in_position} | Daily PnL: ${state['daily_pnl']:.2f}")
            elif cmd == "exit":
                logger.info("Shutting down...")
                break
            else:
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
