"""P3 — Client HTTP minimal pour l'API OpenAQ v3, utilisable depuis plusieurs threads.

Gère la clé API, le User-Agent, les retries (backoff exponentiel), les erreurs 429
et le respect de la limite (60 req/min, 2 000 req/h).

Parallélisme : le client peut être partagé par les threads d'un ThreadPoolExecutor.
  - chaque thread a sa propre session HTTP (requests.Session n'est pas garanti thread-safe) ;
  - un limiteur commun (SharedRateLimiter) met TOUS les threads en pause quand le quota
    annoncé par le serveur (en-têtes X-Ratelimit-*) est presque épuisé, ou après un 429.

Responsable : P3.
"""
import logging
import threading
import time

import requests

BASE_URL = "https://api.openaq.org/v3"
DEFAULT_USER_AGENT = "UrbanHub-school-project/0.1"
MAX_429 = 90  # ~1 h d'attente maximum (90 fenêtres de quota) avant d'abandonner une requête

log = logging.getLogger(__name__)


class SharedRateLimiter:
    """Pause commune à tous les threads, pilotée par les en-têtes de quota du serveur.

    Le serveur compte les requêtes par fenêtre fixe (X-Ratelimit-Reset = secondes avant
    la remise à zéro). Quand il reste moins de `margin` requêtes (margin ≥ nb de threads,
    pour couvrir les requêtes déjà en vol), on bloque tout le monde jusqu'à la remise à zéro.
    """

    def __init__(self, margin=2):
        self.margin = margin
        self._lock = threading.Lock()
        self._pause_until = 0.0

    def wait(self):
        """À appeler avant chaque requête : attend la fin d'une éventuelle pause commune."""
        while True:
            with self._lock:
                delay = self._pause_until - time.monotonic()
            if delay <= 0:
                return
            time.sleep(delay)

    def pause(self, seconds, reason):
        """Déclare une pause commune (la plus longue l'emporte)."""
        with self._lock:
            until = time.monotonic() + seconds
            if until > self._pause_until:
                self._pause_until = until
                log.info("%s : pause commune de %ss", reason, seconds)

    def update(self, headers):
        """Met à jour l'état à partir des en-têtes X-Ratelimit-* d'une réponse."""
        remaining = int(headers.get("X-Ratelimit-Remaining", 100))
        if remaining <= self.margin:
            reset = int(headers.get("X-Ratelimit-Reset", 60)) + 1
            self.pause(reset, f"Quota OpenAQ presque atteint ({remaining} restantes)")


class OpenAQClient:
    """Client OpenAQ v3 avec gestion des limites de débit, partageable entre threads."""

    def __init__(self, api_key, user_agent=DEFAULT_USER_AGENT, timeout=60, max_retries=5, workers=1):
        if not api_key:
            raise ValueError("OPENAQ_API_KEY manquante : renseignez-la dans le fichier .env")
        self.headers = {"X-API-Key": api_key, "User-Agent": user_agent}
        self.timeout = timeout
        self.max_retries = max_retries
        self.limiter = SharedRateLimiter(margin=workers + 1)
        self._local = threading.local()
        self._count_lock = threading.Lock()
        self.n_requests = 0

    @property
    def session(self):
        """Une session HTTP par thread (réutilise les connexions TCP)."""
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
            self._local.session.headers.update(self.headers)
        return self._local.session

    def get(self, path, params=None):
        """GET sur un endpoint (ex. '/locations') ; renvoie le JSON décodé."""
        url = f"{BASE_URL}{path}"
        attempt, n_429 = 0, 0
        while attempt < self.max_retries:
            self.limiter.wait()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                with self._count_lock:
                    self.n_requests += 1
            except requests.RequestException as exc:
                attempt += 1
                wait = 2 ** attempt
                log.warning("Erreur réseau sur %s (%s), nouvel essai dans %ss", path, exc, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                # quota dépassé : ce n'est pas une panne, on attend sans consommer de tentative
                n_429 += 1
                if n_429 > MAX_429:
                    raise RuntimeError(f"Trop de réponses 429 sur {url} (quota horaire épuisé ?)")
                self.limiter.pause(int(resp.headers.get("X-Ratelimit-Reset", 60)) + 1,
                                   f"429 Too Many Requests sur {path}")
                continue
            if resp.status_code >= 500:
                attempt += 1
                wait = 2 ** attempt
                log.warning("Erreur serveur %s sur %s, nouvel essai dans %ss", resp.status_code, path, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()  # 4xx autres que 429 : erreur définitive (clé invalide, 404...)
            self.limiter.update(resp.headers)
            return resp.json()

        raise RuntimeError(f"Échec après {self.max_retries} essais : {url}")

    def paginate(self, path, params=None, limit=1000):
        """Itère sur tous les résultats d'un endpoint paginé (limit max OpenAQ : 1000)."""
        page = 1
        while True:
            data = self.get(path, {**(params or {}), "limit": limit, "page": page})
            results = data.get("results", [])
            yield from results
            if len(results) < limit:
                return
            page += 1
