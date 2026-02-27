// functions/analytics/src/main.js
import { Client, Databases, Query } from "node-appwrite";
import dotenv from "dotenv";
dotenv.config();

// ─── Shared constants ────────────────────────────────────────────────────────

const DESIGNATIONS = [
  "Senior Travel Consultant",
  "Travel Consultant",
  "Junior Travel Consultant",
  "Branch Head",
  "Branch Director",
  "CEO",
];

const DB_ID = process.env.VITE_APPWRITE_DATABASE_ID;
const EMPLOYEES_COL = process.env.VITE_APPWRITE_EMPLOYEE_COLLECTION_ID;
const BOOKINGS_COL = process.env.VITE_APPWRITE_BOOKING_COLLECTION_ID;

// ─── Shared helpers ───────────────────────────────────────────────────────────

/**
 * Paginate through all documents when result count may exceed Appwrite's
 * per-request limit. Returns a flat array of all documents.
 */
async function fetchAllDocuments(db, collectionId, queries, pageLimit = 2000) {
  let all = [];
  let cursor = null;

  while (true) {
    const q = [...queries, Query.limit(pageLimit)];
    if (cursor) q.push(Query.cursorAfter(cursor));

    const { documents, total } = await db.listDocuments(DB_ID, collectionId, q);
    console.log("total", total);
    all = all.concat(documents);

    if (documents.length < pageLimit) break; // last page
    cursor = documents[documents.length - 1].$id;
  }

  return all;
}

/** Parse target JSON strings stored on employee documents */
function parseTargets(employee, year, quarter) {
  // console.log('employee, year, quarter', employee.name, year, quarter)

  const raw = employee.targets?.find((t) => {
    const p = JSON.parse(t);
    return p.year === String(year) && p.quarter === quarter;
  });
  // console.log('raw', raw)
  return raw ? JSON.parse(raw) : null;
}

/** Aggregate raw booking documents into a consultant-keyed map */
function aggregateBookings(bookings) {
  const map = {};
  console.log("bookings", bookings.length);
  bookings.forEach(({ salesHandleName, bookingValue, finalMargin }) => {
    const value = parseInt(bookingValue) || 0;
    const margin = parseInt(finalMargin) || 0;

    if (map[salesHandleName]) {
      map[salesHandleName].revenue += value;
      map[salesHandleName].bookingAchieved += 1;
      map[salesHandleName].marginAchieved += margin;
    } else {
      map[salesHandleName] = {
        name: salesHandleName,
        revenue: value,
        bookingAchieved: 1,
        marginAchieved: margin,
      };
    }
  });
  return map;
}

// ─── Handler: Leaderboard ─────────────────────────────────────────────────────

/**
 * payload: { branch?: string, year: number, quarter: string,
 *            monthFrom: number, monthTo: number }
 * returns: { byBookings: ConsultantMetric[], byMargin: ConsultantMetric[] }
 */
async function handleLeaderboard(db, payload) {
  console.log(
    "handleLeaderboard called with payload: " + JSON.stringify(payload),
  );
  try {
    const { branch, year, quarter, monthFrom, monthTo, targetYear, accesskey, topPerformer } =
      payload;

      const  designation = topPerformer ? DESIGNATIONS.filter(d=>d!=='CEO') : DESIGNATIONS;
    const empQuery = [
      Query.select(["name", "$id", "targets"]),
      Query.contains("designation", designation),
    ];
    if (branch) empQuery.push(Query.equal("branch", branch));

    const employees = await fetchAllDocuments(db, EMPLOYEES_COL, empQuery);
    console.log("Found employees: " + employees);

    const targetMap = {};
    employees.forEach((emp) => {
      const target = parseTargets(emp, targetYear, quarter);
      if (target) targetMap[emp.name] = target;
    });

    //  console.log("Target map: " + JSON.stringify(targetMap, null, 2));
    const names = employees.map((e) => e.name);

    const bookings = await fetchAllDocuments(db, BOOKINGS_COL, [
      Query.greaterThanEqual(
        accesskey === "booked" ? "bookMonth" : "travelMonth",
        monthFrom,
      ),
      Query.lessThanEqual(
        accesskey === "booked" ? "bookMonth" : "travelMonth",
        monthTo,
      ),
      Query.equal(accesskey === "booked" ? "bookYear" : "travelYear", year),
      Query.equal("bookingCancelled", false),
      Query.equal("salesHandleName", names),
      Query.select([
        "bookingID",
        "bookingValue",
        "finalMargin",
        "bookMonth",
        "bookingCancelled",
        "salesHandleName",
      ]),
    ]);

    const map = aggregateBookings(bookings);
    console.log("map", map);

    const result = Object.values(map).map((item) => {
      const target = targetMap[item.name];
      const bookingPercentage = Math.round(
        (item.bookingAchieved / (target?.totalBookings || 1)) * 100,
      );
      const marginPercentage = Math.round(
        (item.marginAchieved / (target?.margin || 1)) * 100,
      );
      return {
        ...item,
        bookingTarget: target?.totalBookings ?? null,
        marginTarget: target?.margin ?? null,
        bookingPercentage,
        marginPercentage,
        isBookingExceeded: bookingPercentage > 100,
        isMarginExceeded: marginPercentage > 100,
      };
    });

    //  console.log("Result: " + JSON.stringify(result, null, 2));

    return {
      byBookings: [...result].sort(
        (a, b) => b.bookingAchieved - a.bookingAchieved,
      ),
      byMargin: [...result].sort((a, b) => b.marginAchieved - a.marginAchieved),
    };
  } catch (error) {
    console.log("Error in handleLeaderboard: " + error);
    return {
      error: true,
      byBookings: [],
      byMargin: [],
    };
  }
}
async function handleCountryWise(db, payload) {
  try {
    const {
      branch,
      startDate,
      endDate,
    } = payload;

    const empQuery = [
      Query.select(["name", "$id"]),
      Query.contains("designation", DESIGNATIONS),
    ];
    if (branch) empQuery.push(Query.equal("branch", branch));

    const employees = await fetchAllDocuments(db, EMPLOYEES_COL, empQuery);
    // console.log("Found employees: " + employees);

    //  console.log("Target map: " + JSON.stringify(targetMap, null, 2));
    const names = employees.map((e) => e.name);

    const bookings = await fetchAllDocuments(db, BOOKINGS_COL, [
      Query.equal("salesHandleName", names),
      Query.greaterThanEqual("bookedDate", startDate),
      Query.lessThanEqual("bookedDate", endDate),
      Query.equal("bookingCancelled", false),
      Query.select([
        "status",
        "destination",
        "salesHandleName",
        "adults",
        "children",'countries'
      ]),
    ]);

    const countryMapping = {};
    const countryAssigneeMapping = {};
    const countryTravelerMapping = {};
    const countryAssigneeTravelerMapping = {};
    const dubaiMapping = ["Dubai", "DUBAI", "United Arab Emirates"];
    const singaporeMapping = ["Singapore", "SINGAPORE", "SIN", "SG"];

    bookings.forEach((req) => {
      const countryNames = req.countries

      if (Array.isArray(countryNames) && countryNames.length > 0) {
        countryNames.forEach((country) => {
          if (dubaiMapping.includes(country.trim())) {
            country = "Dubai";
          }

          if (singaporeMapping.includes(country.trim())) {
            country = "Singapore";
          }

          // compute travellers for this booking
          const travellers =
            (Number(req.adults) || 0) + (Number(req.children) || 0);

          // total booking count
          countryMapping[country] = (countryMapping[country] || 0) + 1;
          // per-salesperson booking count
          const assignee = req.salesHandleName || "Unknown";
          countryAssigneeMapping[country] =
            countryAssigneeMapping[country] || {};
          countryAssigneeMapping[country][assignee] =
            (countryAssigneeMapping[country][assignee] || 0) + 1;

          // total travellers per country
          countryTravelerMapping[country] =
            (countryTravelerMapping[country] || 0) + travellers;
          // per-salesperson travellers per country
          countryAssigneeTravelerMapping[country] =
            countryAssigneeTravelerMapping[country] || {};
          countryAssigneeTravelerMapping[country][assignee] =
            (countryAssigneeTravelerMapping[country][assignee] || 0) +
            travellers;
        });
      }
    });
    // build array with per-country assignee maps and traveller counts
    const allCountries = Object.entries(countryMapping).map(
      ([name, count]) => ({
        name,
        count: Number(count) || 0,
        assignees: countryAssigneeMapping[name] || {},
        travelerCount: Number(countryTravelerMapping[name] || 0),
        assigneeTravellers: countryAssigneeTravelerMapping[name] || {},
      }),
    );

    // sort descending by count
    allCountries.sort((a, b) => b.count - a.count);

    const topN = 10;
    const topCountries = allCountries.slice(0, topN);
    const rest = allCountries.slice(topN);
    if (rest.length > 0) {
      const othersCount = rest.reduce((sum, c) => sum + (c.count || 0), 0);
      // merge assignees across rest into one map for "Others" (bookings and travellers)
      const othersAssignees = {};
      const othersAssigneeTravellers = {};
      const othersTravellerSum = rest.reduce(
        (sum, c) => sum + (c.travelerCount || 0),
        0,
      );

      rest.forEach((c) => {
        const map = c.assignees || {};
        Object.entries(map).forEach(([assignee, cnt]) => {
          othersAssignees[assignee] =
            (othersAssignees[assignee] || 0) + (cnt || 0);
        });
        const tmap = c.assigneeTravellers || {};
        Object.entries(tmap).forEach(([assignee, cnt]) => {
          othersAssigneeTravellers[assignee] =
            (othersAssigneeTravellers[assignee] || 0) + (cnt || 0);
        });
      });

      if (othersCount > 0) {
        topCountries.push({
          name: "Others",
          count: othersCount,
          assignees: othersAssignees,
          travelerCount: othersTravellerSum,
          assigneeTravellers: othersAssigneeTravellers,
        });
      }
    }

     console.log("Result: " + JSON.stringify(topCountries, null, 2));

    return {
      topCountries: [...topCountries],
    };
  } catch (error) {
    console.log("Error in handleLeaderboard: " + error);
    return {
      error: true,
      topCountries: [],
    };
  }
}


async function sendWelcomeMessage(db, payload) {
  console.log('payload', payload)
    const myHeaders = new Headers();
    myHeaders.append("apiSecret", process.env.GALLABOX_API_SECRET);
    myHeaders.append("apiKey", process.env.GALLABOX_API_KEY);
    myHeaders.append("Content-Type", "application/json");
    const raw = JSON.stringify({
      "channelId": process.env.GALLABOX_WELCOME_CHANNEL_ID,
      "channelType": "whatsapp",
      "recipient": {
        "name": payload.name,
        "phone": payload.phone
      },
      "whatsapp": {
        "type": "template",
        "template": {
          "templateName": "welcome_user",
          "buttonValues": [
            {
              "index": 0,
              "sub_type": "quick_reply",
              "parameters": {
                "type": "payload",
                "payload": "Get Free Consultation"
              }
            },
            {
              "index": 1,
              "sub_type": "COPY_CODE",
              "parameters": {
                "type": "coupon_code",
                "coupon_code": "FLAT1000"
              }
            }
          ]
        }
      }
    });

    const requestOptions = {
      method: "POST",
      headers: myHeaders,
      body: raw,
      redirect: "follow"
    };

  try {
    // await the fetch call so `res` is a Response, not a Promise
    const res = await fetch("https://server.gallabox.com/devapi/messages/whatsapp", requestOptions);
    const resultText = await res.text();

    // handle non-2xx responses explicitly
    if (!res.ok) {
      return {
        success: false,
        status: res.status,
        response: resultText,
      };
    }

    return {
      success: true,
      response: resultText,
    };

  } catch (error) {
    return {
      success: false,
      error: error.message,
    };
  }
  }
// ─── Router ───────────────────────────────────────────────────────────────────

const HANDLERS = {
  leaderboard: handleLeaderboard,
  countryWise: handleCountryWise,
  welcomeMessage:sendWelcomeMessage
  // Register new pages here as you build them
};

// ─── Entry point ──────────────────────────────────────────────────────────────

export default async ({ req, res, log, error }) => {
   log("ENDPOINT: " + process.env.VITE_APPWRITE_URL);
  log("PROJECT:  " + process.env.VITE_APPWRITE_PROJECT_ID);
  log("KEY SET:  " + !!process.env.API_KEY); // logs true/false, never logs the key itself
  log("DB_ID:    " + process.env.VITE_APPWRITE_DATABASE_ID);
  log("EMPLOYEE:    " + process.env.VITE_APPWRITE_EMPLOYEE_COLLECTION_ID);
  log("BOOKING:    " + process.env.VITE_APPWRITE_BOOKING_COLLECTION_ID);
  const { type, payload } = req.body;
  log("Received request:", { type, payload });

  if (!type || !HANDLERS[type]) {
    return res.json({ error: `Unknown type "${type}". Valid types: ${Object.keys(HANDLERS).join(", ")}` }, 400);
  }

  const client = new Client()
    .setEndpoint(process.env.VITE_APPWRITE_URL)
    .setProject(process.env.VITE_APPWRITE_PROJECT_ID)
    .setKey(process.env.API_KEY);

  const db = new Databases(client);

  try {
    log(`[analytics] type=${type} payload=${JSON.stringify(payload)}`);
    const data = await HANDLERS[type](db, payload);
    return res.json({ success: true, data });
  } catch (err) {
    error(`[analytics] type=${type} failed: ${err.message}`);
    return res.json({ success: false, error: err.message }, 500);
  }
};

// const start = async (body) => {
//   // console.log("body", body);
//   const { type, payload } = body;
//   console.log('payload', payload)

//   if (!type || !HANDLERS[type]) {
//     return {
//       error: `Unknown type "${type}". Valid types: ${Object.keys(HANDLERS).join(", ")}`,
//     };
//   }

//   const client = new Client()
//     .setEndpoint(process.env.VITE_APPWRITE_URL)
//     .setProject(process.env.VITE_APPWRITE_PROJECT_ID)
//     .setKey(process.env.API_KEY);

//   const db = new Databases(client);

//   try {
//     // console.log(`[analytics] type=${type} payload=${JSON.stringify(payload)}`);
//     const data = await HANDLERS[type](db, payload);
//     return { success: true, data };
//   } catch (err) {
//     // console.log(`[analytics] type=${type} failed: ${err.message}`);
//     return { success: false, error: err.message };
//   }
// };

// const result = await start({
//   type: "welcomeMessage",
//   payload: {
//     name: "Test Vicky",
//     phone:'916383756188'
//   },
// });
// const result = await start({
//   type: "countryWise",
//   payload: {
//     branch: "",
//     startDate:new Date('Sun Feb 01 2026 00:00:00 GMT+0530 (India Standard Time)'),
//     endDate:new Date('Sun Feb 28 2026 23:59:59 GMT+0530 (India Standard Time)'),
//   },
// });
// const result = await start({
//   type: "leaderboard",
//   payload: {
//     branch: "",
//     year: 2026,
//     quarter: "Q4",
//     monthFrom: 1,
//     monthTo: 3,
//     targetYear: "2025",
//     accesskey: "travel",
//   },
// });

// console.log("result", result);
