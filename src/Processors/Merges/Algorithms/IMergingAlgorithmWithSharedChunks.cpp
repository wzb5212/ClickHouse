#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>

namespace DB
{

IMergingAlgorithmWithSharedChunks::IMergingAlgorithmWithSharedChunks(
    size_t num_inputs,
    SortDescription description_,
    WriteBuffer * out_row_sources_buf_,
    size_t max_row_refs)
    : description(std::move(description_))
    , chunk_allocator(num_inputs + max_row_refs)
    , cursors(num_inputs)
    , sources(num_inputs)
    , out_row_sources_buf(out_row_sources_buf_)
{
}

static void prepareChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();

//    Columns Chunk::detachColumns()
//    {
//        num_rows = 0;
//        return std::move(columns);
//    }

    /// std::move is used to indicate that an object t may be "moved from", i.e. allowing the efficient transfer of resources from t to another object.
    /// In particular, std::move produces an xvalue expression that identifies its argument t. It is exactly equivalent to a static_cast to an rvalue reference type.

    auto columns = chunk.detachColumns();

    /// convertToFullColumnIfConst
    /** If column isn't constant, returns itself.
     * If column is constant, transforms constant to full column (if column type allows such transform) and return it.
     */

    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void IMergingAlgorithmWithSharedChunks::initialize(Inputs inputs)
{
    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        if (!inputs[source_num].chunk)
            continue;

        /// convertToFullColumnIfConst
        prepareChunk(inputs[source_num].chunk);

//        struct Source
//        {
//            detail::SharedChunkPtr chunk;
//            bool skip_last_row;
//        };

        /// Sources currently being merged.
//        using Sources = std::vector<Source>;
//        Sources sources;

        auto & source = sources[source_num];

        /// It is a flag which says that last row from chunk should be ignored in result.
        /// This row is not ignored in sorting and is needed to synchronize required source
        /// between different algorithm objects in parallel FINAL.
        /// default inputs.skip_last_row = false;

        source.skip_last_row = inputs[source_num].skip_last_row;
        source.chunk = chunk_allocator.alloc(inputs[source_num].chunk);

        /** Cursor allows to compare rows in different blocks (and parts).
          * Cursor moves inside single block.
          * It is used in priority queue.
          */
//        struct SortCursorImpl
//        {
//            ColumnRawPtrs sort_columns;
//            ColumnRawPtrs all_columns;
//            SortDescription desc;
//            size_t sort_columns_size = 0;
//            size_t rows = 0;
//
//            /** Determines order if comparing columns are equal.
//              * Order is determined by number of cursor.
//              *
//              * Cursor number (always?) equals to number of merging part.
//              * Therefore this field can be used to determine part number of current row (see ColumnGathererStream).
//              */
//            size_t order = 0;
//
//            using NeedCollationFlags = std::vector<UInt8>;
//
//            /** Should we use Collator to sort a column? */
//            NeedCollationFlags need_collation;
//
//            /** Is there at least one column with Collator. */
//            bool has_collation = false;
//
//            /** We could use SortCursorImpl in case when columns aren't sorted
//              *  but we have their sorted permutation
//              */
//            IColumn::Permutation * permutation = nullptr;

        /// using SortCursorImpls = std::vector<SortCursorImpl>;

        cursors[source_num] = SortCursorImpl(source.chunk->getColumns(), description, source_num, inputs[source_num].permutation);

        source.chunk->all_columns = cursors[source_num].all_columns;
        source.chunk->sort_columns = cursors[source_num].sort_columns;
    }

    queue = SortingHeap<SortCursor>(cursors);
}

void IMergingAlgorithmWithSharedChunks::consume(Input & input, size_t source_num)
{
    prepareChunk(input.chunk);

    auto & source = sources[source_num];
    source.skip_last_row = input.skip_last_row;
    source.chunk = chunk_allocator.alloc(input.chunk);
    cursors[source_num].reset(source.chunk->getColumns(), {}, input.permutation);

    source.chunk->all_columns = cursors[source_num].all_columns;
    source.chunk->sort_columns = cursors[source_num].sort_columns;

    queue.push(cursors[source_num]);
}

}
