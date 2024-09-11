import string
import pandas as pd
from typing import Iterable
from TableMiner import Utils as utils
from d3l.input_output.dataloaders import CSVDataLoader
import os
import datetime
from typing import Any
from enum import Enum
import random
import statistics
from d3l.utils.constants import STOPWORDS
from nltk.stem import WordNetLemmatizer

"""
This combines the features to detection subject columns of tables
both in tableMiner+ and recovering semantics of data on the web
1. column_type_judge: judge the type of column:
    named_entity, long_text, number, date_expression, empty, other
"""


class ColumnType(Enum):
    Invalid = -1
    long_text = 0
    named_entity = 1
    number = 2
    date_expression = 3
    empty = 4
    other = 5


class ColumnDetection:
    def __init__(self, values: Iterable[Any], column_type=None):

        if not isinstance(values, pd.Series):
            values = pd.Series(values)
        self.column = values
        if column_type is None:
            self.col_type = self.column_type_judge(50)
        else:
            self.col_type = column_type

        '''
        feature used in the subject column detection
       
        emc: fraction of empty cells
        uc: fraction of cells with unique content 
        ac: if over 50% cells contain acronym or id
        df: distance from the first NE-column
        cm: context match score
        ws: web search score
        
        additional ones in the recovering semantics of data 
        on the web
        
        tlc: average number of words in each cell
        vt: variance in the number of data tokens in each cell
        '''
        self.emc = 0
        self.uc = 0
        self.ac = 0
        self.df = 0
        self.cm = 0
        self.ws = 0
        
        # Only in web
        # TODO: Try with webSearch in true
        self.tlc = 0
        self.vt = 0
        self.acronym_id_num = 0

    def column_type_judge(self, fraction=200):
        """
        Check the type of given column's data type.

        Parameters
        NOTE: I add the tokenize the column step in this utilstion,
        maybe in the future I need to extract it as an independent utilstion
        ----------
        values :  Iterable[Any] A collection of values.
        Returns
        -------
        bool
           All non-null values are long text or not (True/False).
        """
        col_type = -1
        type_count = [0, 0, 0, 0, 0, 0]
        total_token_number = 0
        temp_count_text_cell = 0
        try:
            if len(self.column) == 0:
                raise ValueError("Column has no cell!")
        except ValueError as e:
            print("column_type_judge terminate.", self.column.name, repr(e))
            pass
        checkpoint = fraction
        # If the amount of rows that we wanted to look for is even larger than the amount of rows
        # of this column, then we will just look for the whole column.
        if checkpoint >= len(self.column):
            checkpoint = len(self.column) - 1
        # Iterate and judge to which category the element belongs to
        for index, element in self.column.items():
            # If this is the last cell, we will just judge the type of this column
            # between named_entity and long_text.
            if index == checkpoint:
                if temp_count_text_cell != 0:
                    average_token_number = total_token_number / temp_count_text_cell
                    # TODO: I think this needs further modification later Currently set to 8 just in case
                    if average_token_number > 8:
                        type_count[ColumnType.long_text.value] = temp_count_text_cell
                    else:
                        type_count[ColumnType.named_entity.value] = type_count[ColumnType.named_entity.value] + temp_count_text_cell
                
                # Takes the type that occupies the most in the column
                col_type = type_count.index(max(type_count))
                break
            # If the cell is empty
            elif utils.is_empty(element):
                type_count[ColumnType.empty.value] += 1
                continue
            # If it's numeric
            elif isinstance(element, int) or isinstance(element, float):
                # TODO: Is strange that all these numbers are considered as years.
                #       I think this needs further modification later
                if isinstance(element, int) and 1000 <= element <= int(datetime.datetime.today().year):
                    type_count[ColumnType.date_expression.value] += 1
                    continue
                # Probably this is redundant as the above check should cover this
                elif utils.is_empty(str(element)):
                    type_count[ColumnType.empty.value] += 1
                    continue
                else:
                    type_count[ColumnType.number.value] += 1
                    continue
            else:
                # Judge string type. It could be a long text, named entity, or other.
                # If the element is a single word
                if len(element.split(" ")) == 1:
                    # Judge if it is a null value
                    if utils.is_number(element):
                        # There exists special cases: where year could be recognized as number
                        type_count[ColumnType.number.value] += 1
                        continue
                    # Judge if it is a numeric value
                    elif ',' in element:
                        remove_punctued_elem = element.translate(str.maketrans('', '', ','))
                        if utils.is_number(remove_punctued_elem):
                            type_count[ColumnType.number.value] += 1
                            continue
                    # If non-numeric, judge if it is a date-expression
                    elif utils.is_date_expression(element):
                        type_count[ColumnType.date_expression.value] += 1
                        continue
                    # Judge if it is a single word indicate an entity or a acronym
                    # ".isalpha()" is used to check if the string is only made of letters.
                    elif element.isalpha():
                        # TODO: Actually this is an acronym type. Add a new type for this.
                        if utils.is_acronym(element):
                            if utils.is_country(element):
                                type_count[ColumnType.named_entity.value] += 1
                                continue
                            else:
                                type_count[ColumnType.other.value] += 1
                                continue
                        else:
                            cleaned_text = utils.tokenize_str(element).lower()
                            lemmatizer = WordNetLemmatizer()
                            cannonical_word = lemmatizer.lemmatize(cleaned_text)
                            if cannonical_word not in STOPWORDS:
                                type_count[ColumnType.named_entity.value] += 1
                                continue
                            else:
                                type_count[ColumnType.other.value] += 1
                                continue
                    elif utils.is_valid_url(element):
                        type_count[ColumnType.other.value] += 1
                    else:
                        # Probably this is not necessary since we're inside the block of single word
                        # TODO: Evaluate if this can be removed
                        tokens = utils.token_stop_word(element)
                        if utils.is_acronym(element.translate(str.maketrans('', '', string.digits))):
                            type_count[ColumnType.other.value] += 1
                            continue
                        is_acronym = False
                        for token in tokens:
                            if token.isalpha() and not utils.is_acronym(token):
                                is_acronym = True
                                continue
                        if is_acronym:
                            type_count[ColumnType.named_entity.value] += 1
                        else:
                            type_count[ColumnType.other.value] += 1

                # This is a multi-word element.
                else:
                    token = utils.tokenize_str(element).split(" ")
                    token_with_number = utils.tokenize_with_number(element).split(" ")
                    if utils.is_date_expression(utils.tokenize_with_number(element)):
                        type_count[ColumnType.date_expression.value] += 1
                        continue
                    elif len(token_with_number) == 2:
                        if utils.is_number(token_with_number[0]):
                            type_count[ColumnType.number.value] += 1
                            continue
                    # If it's greater than 3, it's a long text and it will be judged on the last iteration
                    elif len(token) < 3:
                        is_acronym = False
                        for i in token:
                            if utils.is_acronym(i):
                                is_acronym = True
                                break
                        if is_acronym:
                            type_count[ColumnType.other.value] += 1
                            continue
                        else:
                            type_count[ColumnType.named_entity.value] += 1
                            continue
                    else:
                        total_token_number += len(token)
                        temp_count_text_cell += 1

            # stop iteration to the 1/3rd cell and judge what type occupies the most in columns
        self.acronym_id_num = type_count[ColumnType.other.value]
        self.col_type = ColumnType(col_type)
        return self.col_type

    def emc_cal(self):
        """
        Calculate the fraction of empty cells
        Returns none
        -------
        """
        empty_cell_count = 0
        for ele in self.column:
            if utils.is_empty(ele):
                empty_cell_count += 1
        self.emc = empty_cell_count / len(self.column)
        return self.emc

    def uc_cal(self):
        """
        Calculate the fraction of cells with unique text
        a ratio between the number of unique text content and the number of rows
        Returns none
        -------
        """
        column_tmp = self.column
        column_tmp.drop_duplicates()
        self.uc = len(column_tmp) / len(self.column)

    def ac_cal(self):
        """
        indicate if more than 50% cells of a column is acronym
        or id
        -------
        """
        if self.acronym_id_num / len(self.column) > 0.5:
            self.ac = 1

    def df_cal(self, index: int, annotation_dict: dict):
        """
        calculate the df score
        the distance between this NE column and the first NE column
        -------
         """
        first_NE_column_index = [index for index, value in annotation_dict.items() if value == ColumnType.named_entity][
            0]
        if self.col_type == ColumnType.named_entity:
            first_pair = first_NE_column_index
            self.df = index - int(first_pair)

    def cm_cal(self):
        """
        calculate the context match score:
        TableMiner explanation: the frequency of the column header's composing words in the header's context
        note: In the paper they mention the different context include webpage title/table caption and surrounding
        paragraphs, currently our datasets doesn't include this, so we pass this score as 1
        Returns
        -------
        """
        if self.col_type != ColumnType.named_entity or self.col_type != ColumnType.long_text:
            # print("No need to calculate context match score!")
            pass
        else:
            self.cm = 1

    def calculate_cms(self, contexts, context_weights):
        """
        Calculate the context match score for a column header.
        :param contexts: A dictionary where keys are context types (e.g., 'title', 'caption') and
                         values are the text of these contexts.
        :param context_weights: A dictionary where keys are context types and values are the weights for each context.
        :return: The context match score.
        """
        # Tokenize the column header and create a bag-of-words
        if self.col_type != ColumnType.named_entity or self.col_type != ColumnType.long_text:
            # print("No need to calculate context match score!")
            pass
        else:
            bow_header = utils.bow(self.column.name)
            # Initialize the context match score
            cm_score = 0

            # Iterate over each word in the bag-of-words representation of the column header
            for word in bow_header:
                # For each context type
                for context_type, context_text in contexts.items():
                    # Tokenize the context text
                    context_tokens = utils.nltk_tokenize(context_text)
                    # Count the frequency of the word in this context
                    word_freq = context_tokens.count(word)
                    # Add to the context match score, weighted by the context type
                    cm_score += word_freq * context_weights[context_type]
            return cm_score

    def tlc_cal(self):
        """
        variance in the number of data tokens in each cell
        Returns
        -------
        """
        if self.col_type == ColumnType.named_entity:
            token_list = list(self.column.apply((lambda x: len(utils.token_stop_word(x)))))
            self.tlc = statistics.variance(token_list)

    def vt_cal(self):
        self.vt = self.column.apply((lambda x: len(str(x).split(" ")))).sum() / len(self.column)

    def features(self, index: int, annotation_dict):
        self.emc_cal()
        self.uc_cal()
        self.ac_cal()
        self.df_cal(index, annotation_dict)
        self.cm_cal()
        # self.vt_cal()
        # self.tlc_cal()
        # self.vt, self.tlc
        return {'emc': self.emc, 'uc': self.uc, 'ac': self.ac, 'df': self.df, 'cm': self.cm}
