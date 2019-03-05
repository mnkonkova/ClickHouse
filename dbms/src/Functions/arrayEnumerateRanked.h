#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

class FunctionArrayEnumerateUniqRanked;
class FunctionArrayEnumerateDenseRanked;

using DepthType = uint32_t;
using DepthTypes = std::vector<DepthType>;
struct ArraysDepths
{
    DepthType clear_depth;
    DepthTypes depths;
    DepthType max_array_depth;
};

/// Return depth info about passed arrays
ArraysDepths getArraysDepths(const ColumnsWithTypeAndName & arguments);

template <typename Derived>
class FunctionArrayEnumerateRankedExtended : public IFunction
{
public:
    static FunctionPtr create(const Context & /* context */) { return std::make_shared<Derived>(); }

    String getName() const override { return Derived::name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 0)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(arguments.size())
                    + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const ArraysDepths arrays_depths = getArraysDepths(arguments);

        DataTypePtr type = std::make_shared<DataTypeUInt32>();
        for (DepthType i = 0; i < arrays_depths.max_array_depth; ++i)
            type = std::make_shared<DataTypeArray>(type);

        return type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    void executeMethodImpl(
        const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
        const ColumnRawPtrs & columns,
        const ArraysDepths & arrays_depths,
        ColumnUInt32::Container & res_values);
};


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128depths(const std::vector<size_t> & indexes, const ColumnRawPtrs & key_columns)
{
    UInt128 key;
    SipHash hash;

    for (size_t j = 0, keys_size = key_columns.size(); j < keys_size; ++j)
    {
        // Debug: const auto & field = (*key_columns[j])[indexes[j]]; DUMP(j, indexes[j], field);
        key_columns[j]->updateHashWithValue(indexes[j], hash);
    }

    hash.get128(key.low, key.high);

    return key;
}


template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeImpl(
    Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    size_t num_arguments = arguments.size();
    ColumnRawPtrs data_columns;

    Columns array_holders;
    ColumnPtr offsets_column;

    ColumnsWithTypeAndName args;

    for (size_t i = 0; i < arguments.size(); ++i)
        args.emplace_back(block.getByPosition(arguments[i]));

    const auto & arrays_depths = getArraysDepths(args);

    auto get_array_column = [&](const auto & column) -> const DB::ColumnArray *
    {
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(column);
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(column);
            if (!const_array)
                return nullptr;
            array_holders.emplace_back(const_array->convertToFullColumn());
            array = checkAndGetColumn<ColumnArray>(array_holders.back().get());
        }
        return array;
    };

    std::vector<const ColumnArray::Offsets *> offsets_by_depth;
    std::vector<ColumnPtr> offsetsptr_by_depth;

    size_t array_num = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const auto * array = get_array_column(block.getByPosition(arguments[i]).column.get());
        if (!array)
            continue;

        if (array_num == 0) // TODO check with prev
        {
            offsets_by_depth.emplace_back(&array->getOffsets());
            offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
        }
        else
        {
            if (*offsets_by_depth[0] != array->getOffsets())
            {
                throw Exception(
                    "Lengths and depths of all arrays passed to " + getName() + " must be equal.",
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }
        }

        DepthType col_depth = 1;
        for (; col_depth < arrays_depths.depths[array_num]; ++col_depth)
        {
            auto sub_array = get_array_column(&array->getData());
            if (sub_array)
                array = sub_array;
            if (!sub_array)
                break;

            if (offsets_by_depth.size() <= col_depth)
            {
                offsets_by_depth.emplace_back(&array->getOffsets());
                offsetsptr_by_depth.emplace_back(array->getOffsetsPtr());
            }
            else
            {
                if (*offsets_by_depth[col_depth] != array->getOffsets())
                {
                    throw Exception(
                        "Lengths and depths of all arrays passed to " + getName() + " must be equal.",
                        ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
                }
            }
        }

        if (col_depth < arrays_depths.depths[array_num])
        {
            throw Exception(
                getName() + ": Passed array number " + std::to_string(array_num) + " depth ("
                    + std::to_string(arrays_depths.depths[array_num]) + ") more than actual array depth (" + std::to_string(col_depth)
                    + ").",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }

        auto * array_data = &array->getData();
        data_columns.emplace_back(array_data);
        ++array_num;
    }

    if (offsets_by_depth.empty())
        throw Exception("No arrays passed to function " + getName(), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto res_nested = ColumnUInt32::create();

    ColumnUInt32::Container & res_values = res_nested->getData();
    res_values.resize(offsets_by_depth[arrays_depths.max_array_depth - 1]->back());

    executeMethodImpl(offsets_by_depth, data_columns, arrays_depths, res_values);

    ColumnPtr result_nested_array = std::move(res_nested);
    for (int depth = arrays_depths.max_array_depth - 1; depth >= 0; --depth)
        result_nested_array = ColumnArray::create(std::move(result_nested_array), offsetsptr_by_depth[depth]);

    block.getByPosition(result).column = result_nested_array;
}

/*

(2, [[1,2,3],[2,2,1],[3]], 2, [4,5,6], 1)
    ; 1 2 3;  2 2 1;  3        4 5 6
    ; 4 4 4;  5 5 5;  6      <-

(1, [[1,2,3],[2,2,1],[3]], 1, [4,5,6], 1)
    ;[1,2,3] [2,2,1] [3]       4 5 6
    ;4       5       6       <-

(1, [[1,2,3],[2,2,1],[3]], 1, [4,5,6], 0)
    ;[1,2,3] [2,2,1] [3]       4 5 6
    ;[4,5,6] [4,5,6] [4,5,6] <-

. - get data
; - clean index

(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 1)
    ;.                         .                         .

(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2)
    ; .       .       .         .       .       .         .

(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2)
    ; .       .       .       ; .       .       .       ; .

(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3)
    ;  . . .   . . .   . . .     . . .   . . .   . . .     . .

(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3)
    ;  . . .   . . .   . . .  ;  . . .   . . .   . . .  ;  . .

(3, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3)
    ;  . . . ; . . . ; . . .  ;  . . . ; . . . ; . . .  ;  . .

*/

template <typename Derived>
void FunctionArrayEnumerateRankedExtended<Derived>::executeMethodImpl(
    const std::vector<const ColumnArray::Offsets *> & offsets_by_depth,
    const ColumnRawPtrs & columns,
    const ArraysDepths & arrays_depths,
    ColumnUInt32::Container & res_values)
{
    const size_t current_offset_depth = arrays_depths.max_array_depth;
    const auto & offsets = *offsets_by_depth[current_offset_depth - 1];

    ColumnArray::Offset prev_off = 0;

    using Map = ClearableHashMap<
        UInt128,
        UInt32,
        UInt128TrivialHash,
        HashTableGrower<INITIAL_SIZE_DEGREE>,
        HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(UInt128)>>;
    Map indices;

    std::vector<size_t> indexes_by_depth(arrays_depths.max_array_depth);
    std::vector<size_t> current_offset_n_by_depth(arrays_depths.max_array_depth);

    UInt32 rank = 0;

    std::vector<size_t> columns_indexes(columns.size());
    for (size_t off : offsets)
    {
        bool want_clear = false;

        for (size_t j = prev_off; j < off; ++j)
        {
            for (size_t col_n = 0; col_n < columns.size(); ++col_n)
                columns_indexes[col_n] = indexes_by_depth[arrays_depths.depths[col_n] - 1];

            auto hash = hash128depths(columns_indexes, columns);

            if constexpr (std::is_same_v<Derived, FunctionArrayEnumerateUniqRanked>)
            {
                auto idx = ++indices[hash];
                res_values[j] = idx;
            }
            else // FunctionArrayEnumerateDenseRanked
            {
                auto idx = indices[hash];
                if (!idx)
                {
                    idx = ++rank;
                    indices[hash] = idx;
                }
                res_values[j] = idx;
            }

            // Debug: DUMP(off, prev_off, j, columns_indexes, res_values[j], columns);

            for (int depth = current_offset_depth - 1; depth >= 0; --depth)
            {
                ++indexes_by_depth[depth];

                if (indexes_by_depth[depth] == (*offsets_by_depth[depth])[current_offset_n_by_depth[depth]])
                {
                    if (static_cast<int>(arrays_depths.clear_depth) == depth + 1)
                        want_clear = true;
                    ++current_offset_n_by_depth[depth];
                }
                else
                {
                    break;
                }
            }
        }
        if (want_clear)
        {
            want_clear = false;
            indices.clear();
            rank = 0;
        }

        prev_off = off;
    }
}

}
