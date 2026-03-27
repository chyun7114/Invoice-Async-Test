import http from "k6/http";
import { check, sleep } from "k6";
import { FormData } from "https://jslib.k6.io/formdata/0.0.2/index.js";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const MODE = (__ENV.MODE || "sync").toLowerCase();
const ENDPOINT = MODE === "async" ? "/v2/invoice/async" : "/v2/invoice/sync";
const PROFILE = (__ENV.PROFILE || "custom").toLowerCase();

const PROFILES = {
  user: { model: "closed", vus: 2, thinkTimeSec: 1, filesPerRequest: 3, duration: "1m" },
  compare: { model: "arrival", rate: 3, filesPerRequest: 5, duration: "1m", preAllocatedVUs: 10, maxVUs: 50 },
  peak: { model: "closed", vus: 15, thinkTimeSec: 0.3, filesPerRequest: 5, duration: "1m" },
  custom: { model: "closed", vus: 10, thinkTimeSec: 0.2, filesPerRequest: 0, duration: "1m" },
};

const selected = PROFILES[PROFILE] || PROFILES.custom;
const MODEL = __ENV.MODEL || selected.model || "closed";
const THINK_TIME_SEC = Number(__ENV.THINK_TIME_SEC || selected.thinkTimeSec || 0);
const FILES_PER_REQUEST = Number(__ENV.FILES_PER_REQUEST || selected.filesPerRequest);
const DURATION = __ENV.DURATION || selected.duration;
const VUS = Number(__ENV.VUS || selected.vus || 1);
const RATE = Number(__ENV.RATE || selected.rate || 1);
const PRE_ALLOCATED_VUS = Number(__ENV.PRE_ALLOCATED_VUS || selected.preAllocatedVUs || Math.max(10, RATE * 2));
const MAX_VUS = Number(__ENV.MAX_VUS || selected.maxVUs || Math.max(50, RATE * 5));
const RUN_ID = __ENV.RUN_ID || `run-${Date.now()}`;

export const options = MODEL === "arrival"
  ? {
      scenarios: {
        compare_arrival: {
          executor: "constant-arrival-rate",
          rate: RATE,
          timeUnit: "1s",
          duration: DURATION,
          preAllocatedVUs: PRE_ALLOCATED_VUS,
          maxVUs: MAX_VUS,
          tags: {
            profile: PROFILE,
            mode: MODE,
            load_model: "arrival",
          },
        },
      },
      thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<25000"],
      },
    }
  : {
      vus: VUS,
      duration: DURATION,
      thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<25000"],
      },
      tags: {
        profile: PROFILE,
        mode: MODE,
        load_model: "closed",
      },
    };

function shouldThink() {
  return MODEL !== "arrival" && THINK_TIME_SEC > 0;
}

function requestTags() {
  return {
    mode: MODE,
    profile: PROFILE,
    load_model: MODEL,
    files_per_request: String(FILES_PER_REQUEST),
    rate: MODEL === "arrival" ? String(RATE) : "N/A",
    vus: MODEL === "arrival" ? "N/A" : String(VUS),
  };
}

function requestParams() {
  return {
    tags: requestTags(),
  };
}

function postInvoice(url, payload) {
  const params = requestParams();
  if (payload && payload.headers) {
    params.headers = payload.headers;
  }

  return http.post(url, payload ? payload.body : null, params);
}

function sleepIfNeeded() {
  if (shouldThink()) {
    sleep(THINK_TIME_SEC);
  }
}

export default function () {
  const url = `${BASE_URL}${ENDPOINT}`;
  const payload = buildPayload(FILES_PER_REQUEST);

  const res = postInvoice(url, payload);

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleepIfNeeded();
}

function buildPayload(filesPerRequest) {
  if (!Number.isFinite(filesPerRequest) || filesPerRequest <= 0) {
    return null;
  }

  const uniqueBase = `${RUN_ID}-vu${__VU}-iter${__ITER}-ts${Date.now()}`;
  const formData = new FormData();
  for (let i = 0; i < filesPerRequest; i += 1) {
    const unique = `${uniqueBase}-f${i + 1}`;
    formData.append(
      "files",
      http.file(`mock-png-content-${unique}`, `receipt-${unique}.png`, "image/png")
    );
  }

  return {
    body: formData.body(),
    headers: {
      "Content-Type": `multipart/form-data; boundary=${formData.boundary}`,
    },
  };
}
