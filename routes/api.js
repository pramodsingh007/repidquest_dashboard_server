const express = require("express");
const { getDbInstance } = require("../config/db.js");
const router = express.Router();

router.get("/get-customers", async (req, res) => {
  try {
    const db = await getDbInstance();
    const collection = await db.collection("shopifyCustomers");

    const aggregationPipeline = [
      {
        $project: {
          date: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: {
                $toDate: "$created_at",
              },
            },
          },
        },
      },
      {
        $group: {
          _id: "$date",
          newCustomers: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          date: "$_id",
          newCustomers: 1,
        },
      },
      {
        $sort: { date: 1 },
      },
    ];
    const customers = await collection.aggregate(aggregationPipeline).toArray();
    res.json(customers);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});
router.get("/get-repeated-customers", async (req, res) => {
  try {
    const db = await getDbInstance();
    const collection = await db.collection("shopifyOrders");

    const aggregationPipeline = [
      {
        $group: {
          _id: {
            customer_id: "$customer.id",
            date: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: { $dateFromString: { dateString: "$created_at" } },
              },
            },
          },
          ordersCount: { $sum: 1 },
        },
      },
      {
        $match: {
          ordersCount: { $gt: 1 },
        },
      },
      {
        $group: {
          _id: "$_id.date",
          repeatCustomers: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          date: "$_id",
          repeatCustomers: 1,
        },
      },
      {
        $sort: { date: 1 },
      },
    ];
    const customers = await collection.aggregate(aggregationPipeline).toArray();
    res.json(customers);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

router.get("/get-sales", async (req, res) => {
  try {
    const db = await getDbInstance();
    const collection = await db.collection("shopifyOrders");

    const aggregationPipeline = [
      {
        $project: {
          date: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: {
                $toDate: "$created_at",
              },
            },
          },
          total_price: {
            $toDouble: "$total_price",
          },
        },
      },
      {
        $group: {
          _id: "$date",
          totalSales: {
            $sum: "$total_price",
          },
        },
      },
      {
        $project: {
          _id: 0,
          date: "$_id",
          totalSales: { $round: ["$totalSales", 0] },
        },
      },
      {
        $sort: {
          date: 1,
        },
      },
    ];
    const sales = await collection.aggregate(aggregationPipeline).toArray();
    res.json(sales);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

router.get("/get-sales-growth", async (req, res) => {
  try {
    const db = await getDbInstance();
    const collection = await db.collection("shopifyOrders");

    const aggregationPipeline = [
      {
        $project: {
          date: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: {
                $toDate: "$created_at",
              },
            },
          },
          total_price: {
            $toDouble: "$total_price",
          },
        },
      },
      {
        $group: {
          _id: "$date",
          totalSales: {
            $sum: "$total_price",
          },
        },
      },
      {
        $sort: {
          _id: 1,
        },
      },
      {
        $setWindowFields: {
          partitionBy: null,
          sortBy: { _id: 1 },
          output: {
            prevTotalSales: {
              $shift: {
                output: "$totalSales",
                by: -1,
              },
            },
          },
        },
      },
      {
        $project: {
          _id: 0,
          date: "$_id",
          totalSales: { $round: ["$totalSales", 0] },
          growthRate: {
            $cond: {
              if: { $eq: ["$prevTotalSales", null] },
              then: null,
              else: {
                $round: [
                  {
                    $multiply: [
                      {
                        $divide: [
                          { $subtract: ["$totalSales", "$prevTotalSales"] },
                          "$prevTotalSales",
                        ],
                      },
                      100,
                    ],
                  },
                  0,
                ],
              },
            },
          },
        },
      },
    ];
    const sales = await collection.aggregate(aggregationPipeline).toArray();
    res.json(sales);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

router.get("/get-geographical-distribution", async (req, res) => {
  try {
    const db = await getDbInstance();
    const collection = await db.collection("shopifyCustomers");

    const aggregationPipeline = [
      {
        $unwind: "$addresses",
      },
      {
        $group: {
          _id: "$addresses.city",
          value: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          city: "$_id",
          value: 1,
        },
      },
      {
        $sort: {
          city: 1,
        },
      },
    ];
    const geo = await collection.aggregate(aggregationPipeline).toArray();
    res.json(geo);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

router.get("/get-lifetime-value", async (req, res) => {
  try {
    const db = await getDbInstance();
    const collection = await db.collection("shopifyOrders");

    const aggregationPipeline = [
      {
        $group: {
          _id: "$customer.id",
          firstPurchaseDate: {
            $min: {
              $dateFromString: { dateString: "$created_at" },
            },
          },
          lifetimeValue: { $sum: { $toDouble: "$total_price" } },
        },
      },
      {
        $group: {
          _id: {
            cohort: {
              $dateToString: { format: "%Y-%m", date: "$firstPurchaseDate" },
            },
          },
          lifetimeValue: { $sum: "$lifetimeValue" },
        },
      },
      {
        $project: {
          _id: 0,
          cohort: "$_id.cohort",
          lifetimeValue: { $round: ["$lifetimeValue", 0] },
        },
      },
      {
        $sort: { cohort: 1 },
      },
    ];

    const geo = await collection.aggregate(aggregationPipeline).toArray();
    res.json(geo);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

module.exports = router;
