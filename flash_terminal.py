import uuid
import json
import random
import asyncio
import aiohttp
import aiohttp_socks
import logging
import time
import sys
from urllib.parse import urlparse
from typing import Tuple, Optional, List

logging.basicConfig(level = logging.WARNING, format = '%(asctime)s - %(levelname)s - %(message)s')

class colors :
    reset = "\033[0m"
    bold = "\033[1m"
    dim = "\033[2m"
    red = "\033[31m"
    green = "\033[32m"
    yellow = "\033[33m"
    blue = "\033[34m"
    magenta = "\033[35m"
    cyan = "\033[36m"
    white = "\033[37m"

def cprint(text : str, color : str = colors.reset, end : str = "\n") -> None :
    print(f"{color}{text}{colors.reset}", end = end)

def clear_screen() -> None :
    print("\033[2J\033[H", end = "")

def show_banner() -> None :
    clear_screen()
    banner = f"""
{colors.cyan}{colors.bold}
  ███████╗██╗      █████╗ ███████╗██╗  ██╗
  ██╔════╝██║     ██╔══██╗██╔════╝██║  ██║
  █████╗  ██║     ███████║███████╗███████║
  ██╔══╝  ██║     ██╔══██║╚════██║██╔══██║
  ██║     ███████╗██║  ██║███████║██║  ██║
  ╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝
{colors.reset}
{colors.yellow}flash terminal v1.0{colors.reset}
{colors.magenta}team : flash bytes{colors.reset}
{colors.cyan}https://t.me/FlashBytesTeam{colors.reset}
"""
    print(banner)

def show_status(message : str, color : str = colors.cyan) -> None :
    timestamp = time.strftime("%H:%M:%S")
    cprint(f"[{timestamp}] {message}", color)

def show_progress(message : str) -> None :
    sys.stdout.write(f"\r{colors.yellow}⏳ {message}{colors.reset}")
    sys.stdout.flush()

def clear_progress() -> None :
    sys.stdout.write("\r" + " " * 80 + "\r")
    sys.stdout.flush()

def format_proxy(proxy : str) -> str :
    return proxy[:50] + "..." if len(proxy) > 50 else proxy

proxy_api_url = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text"

async def fetch_proxies_async() -> List[str] :
    try :
        async with aiohttp.ClientSession() as sess :
            async with sess.get(proxy_api_url, timeout = aiohttp.ClientTimeout(total = 15)) as response :
                text = await response.text()
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                return lines
    except Exception as e :
        logging.error(f"failed to fetch proxies : {e}")
        return []

async def test_single_proxy_async(proxy_url : str, method : str, url : str, headers : dict, json_data : dict, timeout : int, base_session : aiohttp.ClientSession) -> Tuple[str, Optional[aiohttp.ClientResponse], Optional[aiohttp.ClientSession]] :
    parsed = urlparse(proxy_url)
    scheme = parsed.scheme.lower()
    temp_session = None
    start_time = time.perf_counter()

    try :
        connect_timeout = aiohttp.ClientTimeout(connect = timeout, sock_read = timeout, total = None)

        if scheme in ("socks4", "socks5", "socks5h") :
            connector = aiohttp_socks.ProxyConnector.from_url(proxy_url, rdns = True)
            cookie_jar = aiohttp.CookieJar()
            if base_session and base_session.cookie_jar :
                for cookie in base_session.cookie_jar :
                    cookie_jar.update_cookies({cookie.key : cookie.value})

            temp_session = aiohttp.ClientSession(headers = base_session.headers, cookie_jar = cookie_jar, connector = connector)
            response = await temp_session.request(method = method, url = url, headers = headers, json = json_data, timeout = connect_timeout)
        else :
            response = await base_session.request(method = method, url = url, headers = headers, json = json_data, proxy = proxy_url, timeout = connect_timeout)

        elapsed = time.perf_counter() - start_time

        if response.status == 200 :
            logging.info(f"success via {proxy_url[:40]} ({elapsed:.3f}s)")
            response._should_close = False
            return proxy_url, response, temp_session
        else :
            logging.warning(f"proxy returned {response.status} : {proxy_url[:40]} ({elapsed:.3f}s)")
            response.close()
            if temp_session and not temp_session.closed :
                await temp_session.close()
            return proxy_url, None, None

    except asyncio.CancelledError :
        elapsed = time.perf_counter() - start_time
        logging.warning(f"cancelled {proxy_url[:40]} ({elapsed:.3f}s)")
        if temp_session and not temp_session.closed :
            await temp_session.close()
        raise

    except Exception as e :
        elapsed = time.perf_counter() - start_time
        logging.warning(f"proxy failed {proxy_url[:40]} ({elapsed:.3f}s) : {str(e)[:60]}")
        if temp_session and not temp_session.closed :
            await temp_session.close()
        return proxy_url, None, None

class proxy_manager_async :
    def __init__(self, max_batches : Optional[int] = None, parallel_workers : int = 100) :
        self.proxies = []
        self.current_index = 0
        self.max_batches = max_batches
        self.parallel_workers = parallel_workers

    async def refresh_proxies_async(self) -> None :
        show_progress("fetching proxies...")
        self.proxies = await fetch_proxies_async()
        random.shuffle(self.proxies)
        self.current_index = 0
        clear_progress()
        show_status(f"proxy pool loaded : {len(self.proxies)} proxies", colors.green)

    def get_batch(self, size : int) -> List[str] :
        if self.current_index >= len(self.proxies) :
            return []
        end = min(self.current_index + size, len(self.proxies))
        batch = self.proxies[self.current_index : end]
        self.current_index = end
        return batch

    async def request_with_parallel_retry_async(self, method : str, url : str, **kwargs) -> Tuple[Optional[aiohttp.ClientResponse], Optional[aiohttp.ClientSession]] :
        headers = kwargs.get("headers", {})
        json_data = kwargs.get("json", {})
        timeout = kwargs.pop("timeout", 5)
        base_session = kwargs.get("session")

        if base_session is None :
            raise ValueError("a valid aiohttp session must be passed via 'session'")

        batches_tried = 0

        while self.max_batches is None or batches_tried < self.max_batches :
            batch = self.get_batch(self.parallel_workers)
            if not batch :
                show_status("no proxies in batch, refreshing...", colors.yellow)
                await self.refresh_proxies_async()
                batch = self.get_batch(self.parallel_workers)
                if not batch :
                    show_status("failed to get proxies after refresh", colors.red)
                    continue

            batches_tried += 1
            show_progress(f"testing batch {batches_tried} ({len(batch)} proxies)")

            tasks = [asyncio.create_task(test_single_proxy_async(proxy, method, url, headers, json_data, timeout, base_session)) for proxy in batch]

            pending = set(tasks)

            while pending :
                done, pending = await asyncio.wait(pending, return_when = asyncio.FIRST_COMPLETED, timeout = 10)

                for task in done :
                    try :
                        proxy_url, response, temp_session = task.result()
                        if response is not None :
                            clear_progress()
                            show_status(f"first successful response via {format_proxy(proxy_url)}", colors.green)
                            for p in pending :
                                p.cancel()
                            if pending :
                                await asyncio.gather(*pending, return_exceptions = True)
                            return response, temp_session
                    except asyncio.CancelledError :
                        pass
                    except Exception as e :
                        logging.warning(f"task error : {e}")

                if not done and pending :
                    for p in pending :
                        p.cancel()
                    if pending :
                        await asyncio.gather(*pending, return_exceptions = True)
                    break

            clear_progress()
            show_status("batch failed, trying next batch...", colors.yellow)

        show_status(f"failed to find a working proxy after {batches_tried} batches", colors.red)
        return None, None

base_url = "https://api.qaf.ai"

headers = {
    "Accept" : "*/*",
    "Accept-Language" : "en-US,en;q=0.9",
    "Content-Type" : "application/json",
    "Origin" : "https://qaf.ai",
    "Referer" : "https://qaf.ai/",
    "User-Agent" : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:154.0) Gecko/20100101 Firefox/154.0",
    "x-qaf-client-platform" : "web",
    "x-qaf-client-timezone" : "Africa/Cairo"
}

proxy_manager = proxy_manager_async(max_batches = None, parallel_workers = 100)
aio_session : Optional[aiohttp.ClientSession] = None
message_counter = 0

async def anonymous_sign_in_async() -> bool :
    global aio_session
    if aio_session is None or aio_session.closed :
        aio_session = aiohttp.ClientSession(headers = headers)
    endpoint = f"{base_url}/auth/sign-in/anonymous"
    try :
        async with aio_session.post(endpoint, json = {}) as response :
            if response.status == 200 :
                data = await response.json()
                token = data.get("token", "unknown")
                show_status(f"connected to qaf.ai | token : {token[:12]}...", colors.green)
                return True
            else :
                show_status(f"sign-in failed : {await response.text()}", colors.red)
                return False
    except Exception as e :
        show_status(f"sign-in error : {e}", colors.red)
        return False

async def send_chat_message_async(text_content : str, force_search : bool = True) -> Tuple[Optional[str], bool] :
    global message_counter, aio_session

    if aio_session is None or aio_session.closed :
        raise RuntimeError("session not available, please sign in first")

    message_counter += 1

    if message_counter % 2 == 1 :
        show_status("rotating proxy pool...", colors.cyan)
        await proxy_manager.refresh_proxies_async()

    endpoint = f"{base_url}/chat"

    msg_id = str(uuid.uuid4())
    sub_msg_id = str(uuid.uuid4())

    user_message = text_content.strip()

    if force_search :
        final_text = "use the search tool before answering, gather information from reliable sources, then answer the question : " + user_message
    else :
        final_text = user_message

    payload = {
        "id" : msg_id,
        "message" : {
            "role" : "user",
            "parts" : [
                {
                    "type" : "text",
                    "text" : final_text
                }
            ],
            "id" : sub_msg_id
        },
        "selectedVisibilityType" : "private",
        "incognito" : False
    }

    if force_search :
        payload["tools"] = [
            {
                "type" : "search",
                "function" : {
                    "name" : "search",
                    "description" : "search the islamic library for verses, tafsirs and sources",
                    "parameters" : {
                        "type" : "object",
                        "properties" : {
                            "query" : {
                                "type" : "string",
                                "description" : "search query"
                            },
                            "mode" : {
                                "type" : "string",
                                "enum" : ["semantic", "keyword"],
                                "description" : "search method"
                            },
                            "label" : {
                                "type" : "string",
                                "description" : "short label for the search"
                            }
                        },
                        "required" : ["query"]
                    }
                }
            }
        ]
        payload["tool_choice"] = {"type" : "search"}

    show_status(f"sending message {message_counter} via proxy", colors.cyan)
    response, temp_session = await proxy_manager.request_with_parallel_retry_async("POST", endpoint, headers = headers, json = payload, timeout = 5, session = aio_session)

    if response is None :
        show_status("failed to send request through any proxy", colors.red)
        return None, False

    show_status(f"response status : {response.status}", colors.cyan if response.status == 200 else colors.red)

    if response.status != 200 :
        show_status(f"chat error : {await response.text()}", colors.red)
        response.close()
        if temp_session and not temp_session.closed :
            await temp_session.close()
        return None, False

    show_status("receiving stream...", colors.cyan)

    search_used = False
    full_text = ""

    try :
        buffer = b""
        async for chunk in response.content.iter_any() :
            buffer += chunk
            while b"\n" in buffer :
                line, buffer = buffer.split(b"\n", 1)
                decoded_line = line.decode('utf-8', errors = 'ignore')

                if not decoded_line.startswith("data:") :
                    continue

                data_str = decoded_line[5:].strip()
                if data_str == "[DONE]" :
                    break

                try :
                    event = json.loads(data_str)
                except json.JSONDecodeError :
                    continue

                event_type = event.get("type")

                if event_type == "tool-input-start" and event.get("toolName") == "search" :
                    search_used = True

                if event_type == "text-delta" :
                    full_text += event.get("delta", "")
    except asyncio.TimeoutError :
        logging.warning("timeout while reading response")
    except Exception as e :
        logging.error(f"error while reading stream : {e}")
    finally :
        response.close()
        if temp_session and not temp_session.closed :
            await temp_session.close()

    print("\n" + "=" * 60)
    cprint(f"search used : {search_used}", colors.green if search_used else colors.yellow)
    print("-" * 60)
    cprint("final answer :", colors.bold)
    print(full_text)
    print("=" * 60)

    return full_text, search_used

async def main() -> None :
    show_banner()
    show_status("initializing flash terminal", colors.cyan)

    if not await anonymous_sign_in_async() :
        show_status("failed to sign in, exiting", colors.red)
        return

    print("\n" + colors.dim + "commands : type your question | 'exit' to quit" + colors.reset + "\n")

    try :
        while True :
            user_input = input(f"{colors.bold}flash > {colors.reset}").strip()

            if not user_input :
                continue

            if user_input.lower() in ["exit", "quit", "q"] :
                show_status("terminated by user", colors.yellow)
                break

            await send_chat_message_async(user_input, force_search = True)
            print("\n")
    except KeyboardInterrupt :
        print("\n")
        show_status("terminated by user", colors.yellow)
    finally :
        if aio_session and not aio_session.closed :
            await aio_session.close()
        show_status("session closed", colors.dim)
        cprint("team : flash bytes | https://t.me/FlashBytesTeam", colors.magenta)

if __name__ == "__main__" :
    asyncio.run(main())
