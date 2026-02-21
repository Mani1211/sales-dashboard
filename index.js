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
    console.log('total', total)
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
  console.log('bookings', bookings.length)
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
  console.log("handleLeaderboard called with payload: " + JSON.stringify(payload));
  try {
      const { branch, year, quarter, monthFrom, monthTo, targetYear } = payload;

  const empQuery = [
    Query.select(["name", "$id", "targets"]),
    Query.contains("designation", DESIGNATIONS),
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
    Query.greaterThanEqual("bookMonth", monthFrom),
    Query.lessThanEqual("bookMonth", monthTo),
    Query.equal("bookYear", year),
    Query.equal("bookingCancelled", false),
    Query.equal("salesHandleName", names),
    Query.select(["salesHandleName", "finalMargin", "bookingValue"]),

      // Query.limit(2000),
      //   Query.greaterThanEqual("bookMonth", 1),
      //   Query.lessThanEqual("bookMonth", 3),
      //   Query.equal("bookingCancelled", false),
      //   Query.equal("bookYear", 2026),
      //   Query.select(["bookingID", "salesHandleName", "finalMargin", "bookingValue", "$id"]),
      //   Query.equal("salesHandleName", salespeoples),
  ]);

  const map = aggregateBookings(bookings);

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
    byBookings: [...result].sort((a, b) => b.bookingAchieved - a.bookingAchieved),
    byMargin: [...result].sort((a, b) => b.marginAchieved - a.marginAchieved),
  };
  } catch (error) {
     console.log("Error in handleLeaderboard: " + error);
     return {
      error:true,
    byBookings: [],
    byMargin: []  ,
  };
  }
}

// ─── Handler: Branch Summary ──────────────────────────────────────────────────

/**
 * payload: { year: number, quarter: string, monthFrom: number, monthTo: number }
 * returns: { branches: BranchSummary[] }
 * Use case: A branch-level overview card / comparison table
 */
async function handleBranchSummary(db, payload) {
  const { year, monthFrom, monthTo } = payload;

  const bookings = await fetchAllDocuments(db, BOOKINGS_COL, [
    Query.greaterThanEqual("bookMonth", monthFrom),
    Query.lessThanEqual("bookMonth", monthTo),
    Query.equal("bookYear", year),
    Query.equal("bookingCancelled", false),
    Query.select(["branch", "finalMargin", "bookingValue", "salesHandleName"]),
  ]);

  const branchMap = {};
  bookings.forEach(({ branch, bookingValue, finalMargin, salesHandleName }) => {
    const value = parseInt(bookingValue) || 0;
    const margin = parseInt(finalMargin) || 0;

    if (!branchMap[branch]) {
      branchMap[branch] = {
        branch,
        totalRevenue: 0,
        totalMargin: 0,
        totalBookings: 0,
        uniqueConsultants: new Set(),
      };
    }

    branchMap[branch].totalRevenue += value;
    branchMap[branch].totalMargin += margin;
    branchMap[branch].totalBookings += 1;
    branchMap[branch].uniqueConsultants.add(salesHandleName);
  });

  // Serialize Set before returning
  const branches = Object.values(branchMap).map((b) => ({
    ...b,
    consultantCount: b.uniqueConsultants.size,
    uniqueConsultants: undefined, // strip the Set
  }));

  return { branches: branches.sort((a, b) => b.totalRevenue - a.totalRevenue) };
}

// ─── Handler: Consultant Detail ───────────────────────────────────────────────

/**
 * payload: { consultantName: string, year: number, quarter: string,
 *            monthFrom: number, monthTo: number }
 * returns: { profile, monthlyBreakdown, recentBookings }
 * Use case: Clicking into a specific consultant's performance page
 */
async function handleConsultantDetail(db, payload) {
  const { consultantName, year, quarter, monthFrom, monthTo, targetYear } = payload;

  const employees = await fetchAllDocuments(db, EMPLOYEES_COL, [
    Query.equal("name", consultantName),
    Query.select(["name", "$id", "targets", "branch", "designation"]),
  ]);

  const employee = employees[0] ?? null;
  const target = employee ? parseTargets(employee, targetYear, quarter) : null;

  const bookings = await fetchAllDocuments(db, BOOKINGS_COL, [
    Query.greaterThanEqual("bookMonth", monthFrom),
    Query.lessThanEqual("bookMonth", monthTo),
    Query.equal("bookYear", year),
    Query.equal("bookingCancelled", false),
    Query.equal("salesHandleName", consultantName),
    Query.select(["bookingID", "bookingValue", "finalMargin", "bookMonth", "bookingCancelled"]),
  ]);

  // Month-by-month breakdown
  const monthlyMap = {};
  bookings.forEach(({ bookingValue, finalMargin, bookMonth }) => {
    const value = parseInt(bookingValue) || 0;
    const margin = parseInt(finalMargin) || 0;
    if (!monthlyMap[bookMonth]) {
      monthlyMap[bookMonth] = { month: bookMonth, revenue: 0, margin: 0, bookings: 0 };
    }
    monthlyMap[bookMonth].revenue += value;
    monthlyMap[bookMonth].margin += margin;
    monthlyMap[bookMonth].bookings += 1;
  });

  const totalRevenue = bookings.reduce((s, b) => s + (parseInt(b.bookingValue) || 0), 0);
  const totalMargin = bookings.reduce((s, b) => s + (parseInt(b.finalMargin) || 0), 0);
  const totalBookings = bookings.length;

  return {
    profile: {
      name: employee?.name,
      branch: employee?.branch,
      designation: employee?.designation,
      target,
    },
    summary: {
      totalRevenue,
      totalMargin,
      totalBookings,
      bookingPercentage: Math.round((totalBookings / (target?.totalBookings || 1)) * 100),
      marginPercentage: Math.round((totalMargin / (target?.margin || 1)) * 100),
    },
    monthlyBreakdown: Object.values(monthlyMap).sort((a, b) => a.month - b.month),
    recentBookings: bookings.slice(-10).reverse(), // last 10
  };
}

// ─── Router ───────────────────────────────────────────────────────────────────

const HANDLERS = {
  leaderboard: handleLeaderboard,
  branch_summary: handleBranchSummary,
  consultant_detail: handleConsultantDetail,
  // Register new pages here as you build them
};

// ─── Entry point ──────────────────────────────────────────────────────────────

// export default async ({ req, res, log, error }) => {
//    log("ENDPOINT: " + process.env.VITE_APPWRITE_URL);
//   log("PROJECT:  " + process.env.VITE_APPWRITE_PROJECT_ID);
//   log("KEY SET:  " + !!process.env.API_KEY); // logs true/false, never logs the key itself
//   log("DB_ID:    " + process.env.VITE_APPWRITE_DATABASE_ID);
//   log("EMPLOYEE:    " + process.env.VITE_APPWRITE_EMPLOYEE_COLLECTION_ID);
//   log("BOOKING:    " + process.env.VITE_APPWRITE_BOOKING_COLLECTION_ID);
//   const { type, payload } = req.body;

//   if (!type || !HANDLERS[type]) {
//     return res.json({ error: `Unknown type "${type}". Valid types: ${Object.keys(HANDLERS).join(", ")}` }, 400);
//   }

//   const client = new Client()
//     .setEndpoint(process.env.VITE_APPWRITE_URL)
//     .setProject(process.env.VITE_APPWRITE_PROJECT_ID)
//     .setKey(process.env.API_KEY);

//   const db = new Databases(client);

//   try {
//     log(`[analytics] type=${type} payload=${JSON.stringify(payload)}`);
//     const data = await HANDLERS[type](db, payload);
//     return res.json({ success: true, data });
//   } catch (err) {
//     error(`[analytics] type=${type} failed: ${err.message}`);
//     return res.json({ success: false, error: err.message }, 500);
//   }
// };
const start = async (body) => {
  const { type, payload } = body;

  if (!type || !HANDLERS[type]) {
    return { error: `Unknown type "${type}". Valid types: ${Object.keys(HANDLERS).join(", ")}` };
  }

  const client = new Client()
    .setEndpoint(process.env.VITE_APPWRITE_URL)
    .setProject(process.env.VITE_APPWRITE_PROJECT_ID)
    .setKey(process.env.API_KEY);

  const db = new Databases(client);

  try {
    console.log(`[analytics] type=${type} payload=${JSON.stringify(payload)}`);
    const data = await HANDLERS[type](db, payload);
    return { success: true, data };
  } catch (err) {
    console.log(`[analytics] type=${type} failed: ${err.message}`);
    return { success: false, error: err.message };
  }
};


const result = await start({type:'leaderboard', payload:{
   branch: "", year: 2026, quarter: 'Q4', monthFrom: 1, monthTo: 3, targetYear:'2025'
}})


console.log('result', result)

